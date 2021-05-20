#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
hadoop fs -mkdir s3://bigdata-offline/warehouse/dwd/dwd_vova_fact_pay_h
###逻辑sql
sql="
insert overwrite table dwd.dwd_vova_fact_pay_h
select /*+ REPARTITION(100) */
       oi.project_name datasource,
       og.rec_id  as order_goods_id,
       og.order_goods_sn,
       og.order_id,
       oi.user_id as buyer_id,
       oi.gender,
       oi.email   as buyer_email,
       oi.from_domain,
       oi.payment_id,
       oi.payment_name,
       g.mct_id,
       og.sku_id,
       og.goods_id,
       og.goods_sn,
       og.goods_name,
       g.cat_id,
       g.cat_name,
       g.first_cat_id,
       g.first_cat_name,
       g.second_cat_id,
       g.second_cat_name,
       oi.order_time,
       ogs.confirm_time,
       oi.pay_time,
       ore.device_id,
       case
           when ore.device_type in (0, 23, 24, 25) then 'pc'
           when ore.device_type in (21, 22, 26) then 'mob'
           when ore.device_type = 11 then 'ios'
           when ore.device_type = 12 then 'android'
           else 'unknown'
           end    as platform,
       og.market_price,
       og.goods_number,
       og.shop_price,
       og.shipping_fee,
       og.goods_weight,
       og.bonus,
       og.mct_shop_price,
       og.mct_shipping_fee,
       r.region_id,
       r.region_code
from ods_vova_vts.ods_vova_order_goods_h og
         left join ods_vova_vts.ods_vova_order_info_h oi on oi.order_id = og.order_id
         left join dim.dim_vova_goods g on g.goods_id = og.goods_id
         left join ods_vova_vts.ods_vova_order_goods_status_h ogs on ogs.order_goods_id = og.rec_id
         left join ods_vova_vts.ods_vova_order_relation_h ore on ore.order_id = oi.order_id
         left join ods_vova_vts.ods_vova_region r on r.region_id = oi.country
where oi.pay_status >= 1
  and oi.email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
  and oi.parent_order_id = 0;
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=dwd_vova_fact_pay_h" \
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

if [ $? -ne 0 ];then
  exit 1
fi
