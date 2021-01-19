#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
# 人民币对美元汇率
exchange_rate_202007=7
# 仓储费
storage_fee=50
#指定日期和引擎
cur_date=$1
cur_date_15=$2
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

if [ ! -n "$2" ];then
cur_date_15=`date -d "-15 day" +%Y-%m-%d`
fi

job_name="dwb_vova_finance_self_mct_summary_req4823_chenkai_${cur_date}"

echo "date_start: ${cur_date_15}; date_end: ${cur_date}"
echo "exchange_rate: ${exchange_rate_202007}; storage_fee: ${storage_fee}"
###逻辑sql
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
   AND date(tnew.order_time) >= date_sub('${cur_date}', 120)
)

insert OVERWRITE TABLE dwb.dwb_vova_finance_self_mct_summary PARTITION (pt)
select
    t3.datasource,
    t3.mct_name,
    nvl(t3.confirm_ord_gs_cnt, 0) confirm_ord_gs_cnt,
    nvl(t3.out_warehouse_ord_gs_cnt, 0) out_warehouse_ord_gs_cnt,
    nvl(t3.shipping_rate, 0) shipping_rate,
    nvl(t3.mct_amount, 0) mct_amount,
    nvl(t3.confirm_mct_amount, 0) confirm_mct_amount,
    nvl(t3.refund_mct_amount, 0) refund_mct_amount,
    nvl(t3.shipping_fee, 0) shipping_fee,
    nvl(t3.confirm_mct_shipping_fee, 0) confirm_mct_shipping_fee,
    nvl(t3.refund_mct_shipping_fee, 0) refund_mct_shipping_fee,
    nvl(t3.mct_cost, 0) mct_cost,
    nvl(t3.carrier_cost, 0) carrier_cost,
    nvl(t3.last_waybill_fee, 0) last_waybill_fee,
    nvl(t3.warehouse_operate_fee, 0) warehouse_operate_fee,
    nvl(t3.storage_fee, 0) storage_fee,
    nvl(t3.inventory_loss, 0) inventory_loss,
    nvl(t3.platform_service_fee, 0) platform_service_fee,

    nvl(t3.mct_amount + t3.shipping_fee - t3.mct_cost - t3.carrier_cost - t3.platform_service_fee, 0) gross_margin,
    nvl(t3.mct_amount + t3.shipping_fee - t3.mct_cost - t3.carrier_cost - t3.platform_service_fee - t3.inventory_loss, 0) net_margin,
    nvl(round(t3.refund_mct_amount/t3.confirm_mct_amount, 2), 0) refund_of_shipped_amount_rate,
    nvl(round(t3.shipping_fee/t3.confirm_mct_amount, 2), 0) shipping_of_shipped_amount_rate,
    nvl(round(t3.mct_cost/t3.confirm_mct_amount, 2), 0) mct_cost_of_shipped_amount_rate,
    nvl(round(t3.carrier_cost/t3.confirm_mct_amount, 2), 0) carrier_of_shipped_amount_rate,
    nvl(round(t3.platform_service_fee/t3.confirm_mct_amount, 2), 0) service_of_shipped_amount_rate,
    nvl(round(t3.inventory_loss/t3.confirm_mct_amount, 2), 0) inventory_loss_of_shipped_amount_rate,
    nvl(round((t3.mct_amount + t3.shipping_fee - t3.mct_cost - t3.carrier_cost - t3.platform_service_fee - t3.inventory_loss)/t3.confirm_mct_amount, 2), 0) net_margin_of_shipped_amount_rate,
    t3.pt pt
from
    (select
         nvl(t2.datasource, 'all') datasource,
         nvl(t2.mct_name, 'all') mct_name,
         nvl(t2.pt, 'all') pt,
         sum(t2.confirm_ord_gs_cnt) confirm_ord_gs_cnt,
         sum(t2.out_warehouse_ord_gs_cnt) out_warehouse_ord_gs_cnt,
         round(sum(t2.out_warehouse_ord_gs_cnt)/sum(t2.confirm_ord_gs_cnt), 2) shipping_rate,
         sum(t2.confirm_mct_amount) - sum(t2.refund_mct_amount)  mct_amount,
         sum(t2.confirm_mct_amount) confirm_mct_amount,
         sum(t2.refund_mct_amount) refund_mct_amount,
         sum(t2.confirm_mct_shipping_fee) - sum(t2.refund_mct_shipping_fee) shipping_fee,
         sum(t2.confirm_mct_shipping_fee) confirm_mct_shipping_fee,
         sum(t2.refund_mct_shipping_fee) refund_mct_shipping_fee,
         sum(t2.mct_cost) mct_cost,
         sum(t2.last_waybill_fee) + sum(if(t2.order_goods_shipping_status>=8, 1, 0))*2.3/${exchange_rate_202007} + ${storage_fee} carrier_cost,
         sum(t2.last_waybill_fee) last_waybill_fee,
         sum(if(t2.order_goods_shipping_status>=8, 1, 0))*2.3/${exchange_rate_202007} warehouse_operate_fee,
         ${storage_fee} storage_fee,
         sum(t2.inventory_loss) inventory_loss,
         (sum(t2.confirm_mct_amount) + sum(t2.confirm_mct_shipping_fee)) * 0.145*0.8 platform_service_fee
     from
         (
             select
                 nvl(dog.datasource, 'NA') datasource, -- d_平台数据源
                 dog.order_goods_sn order_goods_sn,
                 nvl(tfsf.mct_name, 'NA') mct_name, -- d_店铺名
                 tfsf.sku_order_status sku_order_status, -- VOVA订单状态（0：未确认，1：已确认，2 ：已取消）
                 dog.sku_shipping_status sku_shipping_status, -- 子订单物流状态 0未发货 1已发货 2已签收
                 fspog.order_goods_shipping_status order_goods_shipping_status, -- 子订单出库状态
                 if(tfsf.sku_order_status in (1,2), 1,0) confirm_ord_gs_cnt, -- 当日已确认订单数量
                 if(fspog.order_goods_shipping_status >=8, 1, 0) out_warehouse_ord_gs_cnt, -- 已出库订单数
                 nvl(tfsf.old_group_price, 0) as display_old_group_price, -- 商品收入里所含商品价格(USD)
                 if(tfsf.sku_order_status in (1,2) and fspog.order_goods_shipping_status >=8, nvl(tfsf.old_group_price, 0), 0) confirm_mct_amount, -- 商品销售收入（发货）
                 if(tfsf.sku_order_status = 2 and fspog.order_goods_shipping_status >=8, nvl(tfsf.old_group_price, 0), 0) refund_mct_amount, -- 商品销售收入（退货）
                 tfsf.shipping_price, -- 商品收入里所含运费（USD)
                 if(tfsf.sku_order_status in (1,2) and fspog.order_goods_shipping_status >=8, nvl(tfsf.shipping_price, 0), 0) confirm_mct_shipping_fee, -- 商家结算价格中的运费价格（发货）
                 if(tfsf.sku_order_status = 2 and fspog.order_goods_shipping_status >=8, nvl(tfsf.shipping_price, 0), 0) refund_mct_shipping_fee, -- 商家结算价格中的运费价格（退货）
                 tfsf.purchase_total_amount purchase_total_amount, -- 商品采购价
                 if(tfsf.sku_order_status in (1,2) and fspog.order_goods_shipping_status >=8, round(tfsf.purchase_total_amount/${exchange_rate_202007}, 2), 0) mct_cost, -- 减：商品成本
                 fspog.plan_freight, -- 预估运费
                 if(tfsf.sku_order_status in (1,2) and fspog.order_goods_shipping_status >=8, round(fspog.plan_freight/${exchange_rate_202007}, 2), 0) last_waybill_fee, -- —尾程运费（集运仓-国外）
                 tfsf.purchase_pay_status purchase_pay_status,
                 if(tfsf.purchase_pay_status = 1 and tfsf.sku_order_status = 2 and dog.sku_shipping_status=0, round(tfsf.purchase_total_amount/${exchange_rate_202007}, 2), 0) inventory_loss, -- 存货跌价损失
                 to_date(dog.order_time) pt
             from dim.dim_vova_order_goods dog
                      left join
                  tmp_financial_self_final tfsf
                  on dog.order_goods_sn =  tfsf.order_goods_sn
                      LEFT JOIN dwd.dwd_vova_fact_supply_order_goods fspog
                                on tfsf.order_goods_sn = fspog.channel_order_goods_sn
             where to_date(dog.order_time) <= '${cur_date}' and to_date(dog.order_time) >= '${cur_date_15}' and dog.mct_id in (26414, 11630, 36655)
         ) t2
     GROUP BY CUBE (t2.datasource,t2.mct_name,t2.pt) HAVING pt != 'all'
    ) t3 where t3.datasource is not NULL and t3.mct_name != 'NA';
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 5G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=100" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"


#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

echo "end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
