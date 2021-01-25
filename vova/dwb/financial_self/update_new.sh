#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

#TEST
sql="
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=1000;
with tmp_financial_self_new as (
SELECT dog.order_time,
       dog.confirm_time,
       tpog.channel_order_goods_sn as order_goods_sn,
       dog.sku_order_status,
       dm.mct_name,
       dog.goods_number,
       dog.first_cat_name,
       dog.cat_id,
       tpog.freight                                                     AS shipping_price,
       tpog.goods_amount                                                AS normal_price,
       tpog.goods_amount                                                AS old_group_price,
       0                                                                AS shipping_fee,
       dog.shop_price * dog.goods_number + dog.shipping_fee AS order_amount,
       dog.mct_shop_price * dog.goods_number + dog.mct_shipping_fee AS mct_order_amount,
       tpogpp.purchase_platform_order_id as pdd_order_sn,
       tpogpp.purchase_platform_parent_order_id as pdd_parent_order_sn,
       tpog.order_goods_status AS sales_order_status,
       tpogpp.purchase_order_status,
       tpogpp.purchase_order_pay_status AS purchase_pay_status,
       tpogpp.purchase_order_shipping_status AS purchase_shipping_status,
       tpogpp.in_inventory_status AS shipping_order_status,
       tpogpp.purchase_amount as purchase_total_amount
FROM ods_gyl_gpg.ods_gyl_order_goods tpog
         INNER JOIN ods_gyl_gpg.ods_gyl_order_info tpoi ON tpoi.order_id = tpog.order_id
         LEFT JOIN
         (
         select
         t1.purchase_platform_order_id,
         t1.purchase_platform_parent_order_id,
         t1.purchase_order_status,
         t1.purchase_order_pay_status,
         t1.purchase_order_shipping_status,
         t1.in_inventory_status,
         t1.purchase_amount,
         t1.order_goods_id
         from
         (
         select
         tpogpp.purchase_platform_order_id,
         tpogpp.purchase_platform_parent_order_id,
         tpogpp.purchase_order_status,
         tpogpp.purchase_order_pay_status,
         tpogpp.purchase_order_shipping_status,
         tpogpp.in_inventory_status,
         tpogpp.purchase_amount,
         tpogpp.order_goods_id,
         row_number() over (partition by order_goods_id order by create_time desc)        as rank
         from
         ods_gyl_gpg.ods_gyl_order_goods_purchase_plan tpogpp
         ) t1
         where t1.rank = 1
         ) tpogpp ON tpogpp.order_goods_id = tpog.order_goods_id
         INNER JOIN dim.dim_vova_order_goods dog ON dog.order_goods_sn = tpog.channel_order_goods_sn
         INNER JOIN dim.dim_vova_merchant dm ON dm.mct_id = dog.mct_id
 WHERE date(dog.order_time) <= '${cur_date}'
   AND date(dog.order_time) >= date_sub('${cur_date}', 120)
),


-- 自营店铺销售采购明细
tmp_financial_self_final as (
SELECT
       'new' AS original_source,
       tnew.order_time,
       tnew.confirm_time,
       tnew.order_goods_sn,
       tnew.sku_order_status,
       tnew.mct_name,
       tnew.goods_number,
       tnew.first_cat_name,
       tnew.cat_id,
       tnew.shipping_price,
       tnew.normal_price,
       tnew.old_group_price,
       tnew.shipping_fee,
       tnew.order_amount,
       tnew.mct_order_amount,
       tnew.pdd_order_sn,
       tnew.pdd_parent_order_sn,
       tnew.sales_order_status,
       tnew.purchase_order_status,
       tnew.purchase_pay_status,
       tnew.purchase_shipping_status,
       tnew.shipping_order_status,
       tnew.purchase_total_amount,
       tnew.normal_price as display_normal_price
from tmp_financial_self_new tnew
WHERE date(tnew.order_time) <= '${cur_date}'
   AND date(tnew.order_time) >= date_sub('${cur_date}', 120))

-- 自营店铺销售采购明细
INSERT OVERWRITE TABLE dwb.dwb_vova_financial_self_process PARTITION (pt)
select
/*+ REPARTITION(1) */
       tnew.original_source,
       tnew.order_time,
       tnew.confirm_time,
       tnew.order_goods_sn,
       tnew.sku_order_status,
       tnew.mct_name,
       tnew.goods_number,
       tnew.first_cat_name,
       tnew.cat_id,
       tnew.shipping_price,
       tnew.normal_price,
       tnew.old_group_price,
       tnew.shipping_fee,
       tnew.order_amount,
       tnew.mct_order_amount,
       tnew.pdd_order_sn,
       tnew.pdd_parent_order_sn,
       tnew.sales_order_status,
       tnew.purchase_order_status,
       tnew.purchase_pay_status,
       tnew.purchase_shipping_status,
       tnew.shipping_order_status,
       tnew.purchase_total_amount,
       nvl(tnew.normal_price, 0) as display_normal_price,
       nvl(tnew.old_group_price, 0) as  display_old_group_price,
       round(nvl(tnew.display_normal_price, 0) + shipping_price + shipping_fee, 2) as  display_price,
       if(fspog.order_goods_shipping_status >=8,'是','否' ) AS out_warehouse_status,
       fspog.last_waybill_no,
       fspog.carrier_code,
       fspog.guess_weight,
       fspog.plan_freight,
       fspog.actual_weight,
       fspog.actual_freight,
       date(tnew.order_time) as pt
from tmp_financial_self_final tnew
LEFT JOIN dwd.dwd_vova_fact_supply_order_goods fspog on tnew.order_goods_sn = fspog.channel_order_goods_sn
;


INSERT OVERWRITE TABLE dwb.dwb_vova_financial_self PARTITION (pt)
SELECT tnew.original_source,
       tnew.order_time,
       tnew.confirm_time,
       tnew.order_goods_sn,
       CASE
           WHEN sku_order_status = 0 THEN '未确认'
           WHEN sku_order_status = 1 THEN '已确认'
           WHEN sku_order_status = 2 THEN '已取消'
           WHEN sku_order_status = 5 THEN '已转移'
           ELSE sku_order_status END           AS sku_order_status,
       tnew.mct_name,
       tnew.goods_number,
       tnew.first_cat_name AS first_cat_name,
       tnew.cat_id,
       tnew.shipping_price,
       tnew.display_old_group_price,
       tnew.display_normal_price,
       tnew.shipping_fee,
       tnew.display_price,
       tnew.order_amount,
       tnew.mct_order_amount,
       tnew.pdd_order_sn,
       tnew.pdd_parent_order_sn,
       CASE
           WHEN sales_order_status = 0 THEN '未确认'
           WHEN sales_order_status = 1 THEN '已确认'
           WHEN sales_order_status = 2 THEN '已取消'
           ELSE sales_order_status END         AS sales_order_status,
       CASE
           WHEN original_source = 'old' AND purchase_order_status = 0 THEN '未确认'
           WHEN original_source = 'old' AND purchase_order_status = 1 THEN '已确认'
           WHEN original_source = 'old' AND purchase_order_status = 2 THEN '已取消'
           WHEN original_source = 'old' AND purchase_order_status = 4 THEN '拍单失败'
           WHEN original_source = 'new' AND purchase_order_status = 0 THEN '未确认'
           WHEN original_source = 'new' AND purchase_order_status = 1 THEN '已确认'
           WHEN original_source = 'new' AND purchase_order_status = 2 THEN '已取消'
           WHEN original_source = 'new' AND purchase_order_status = 3 THEN '拍单失败'
           WHEN original_source = 'new' AND purchase_order_status = 4 THEN '拍单中'
           WHEN original_source = 'new' AND purchase_order_status = 5 THEN '已拍单'
           WHEN original_source = 'new' AND purchase_order_status = 6 THEN '已重新生成'
           WHEN original_source = 'new' AND purchase_order_status = 7 THEN '最终取消'
           ELSE purchase_order_status END      AS purchase_order_status,
       CASE
           WHEN original_source = 'new' AND purchase_pay_status = 0 THEN '未付款'
           WHEN original_source = 'new' AND purchase_pay_status = 1 THEN '未付款'
           WHEN original_source = 'new' AND purchase_pay_status = 2 THEN '已付款'
           WHEN original_source = 'new' AND purchase_pay_status = 3 THEN '已退款'
           WHEN original_source = 'new' AND purchase_pay_status = 4 THEN '待退款'
           WHEN original_source = 'new' AND purchase_pay_status = 5 THEN '审核不通过'
           WHEN original_source = 'new' AND purchase_pay_status = 6 THEN '待审核'
           WHEN original_source = 'old' AND purchase_pay_status = 0 THEN '未付款'
           WHEN original_source = 'old' AND purchase_pay_status = 1 THEN '付款中'
           WHEN original_source = 'old' AND purchase_pay_status = 2 THEN '已付款'
           WHEN original_source = 'old' AND purchase_pay_status = 4 THEN '已退款'
           WHEN original_source = 'old' AND purchase_pay_status = 30 THEN '退款待审核'
           ELSE purchase_pay_status END        AS purchase_pay_status,
       CASE
           WHEN original_source = 'new' AND purchase_shipping_status = 0 THEN '未发货'
           WHEN original_source = 'new' AND purchase_shipping_status = 1 THEN '未发货'
           WHEN original_source = 'new' AND purchase_shipping_status = 2 THEN '已发货'
           WHEN original_source = 'new' AND purchase_shipping_status = 3 THEN '已妥投'
           WHEN original_source = 'old' AND purchase_shipping_status = 0 THEN '未发货'
           WHEN original_source = 'old' AND purchase_shipping_status = 1 THEN '已发货'
           WHEN original_source = 'old' AND purchase_shipping_status = 2 THEN '已妥投'
           ELSE purchase_shipping_status END   AS purchase_shipping_status,
       CASE
           WHEN original_source = 'new' AND shipping_order_status = 0 THEN '未入库'
           WHEN original_source = 'new' AND shipping_order_status = 1 THEN '未入库'
           WHEN original_source = 'new' AND shipping_order_status = 2 THEN '入库成功'
           WHEN original_source = 'new' AND shipping_order_status = 3 THEN '入库失败'
           WHEN original_source = 'old' AND shipping_order_status = 0 THEN '初始状态未发送发货指令'
           WHEN original_source = 'old' AND shipping_order_status = 1 THEN '已发送发货指令'
           WHEN original_source = 'old' AND shipping_order_status = 2 THEN '货物出库成功'
           WHEN original_source = 'old' AND shipping_order_status = 3 THEN '货物出库失败/或者取消'
           ELSE shipping_order_status END AS shipping_order_status,
       tnew.purchase_total_amount,
       tnew.out_warehouse_status,
       tnew.last_waybill_no,
       tnew.carrier_code,
       tnew.guess_weight,
       tnew.plan_freight,
       tnew.actual_weight,
       tnew.actual_freight,
       date(tnew.order_time) as pt
FROM  dwb.dwb_vova_financial_self_process tnew


"
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=20" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=dwb_vova_financial_self" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi