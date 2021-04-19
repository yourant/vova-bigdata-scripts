#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

sql="
select
oi.coupon_code,
from_unixtime(oc.coupon_ctime) as cpn_create_time,
-- from_unixtime(oc.coupon_ctime + oc.extend_day *24 * 3600) as cpn_create_time,
oi.order_time,
oi.order_sn,
oi.email,
oi.order_amount,
-oi.bonus,
sm.sm_desc as shipping_method_name,
r.region_code,
kce.contact_email,
kce.fans_coupon_code
from
ods_vova_vts.ods_vova_order_info oi
inner join ods_vova_vts.ods_vova_ok_coupon oc on oc.coupon_code = oi.coupon_code
inner join ods_vova_vts.ods_vova_ok_coupon_config occ on oc.coupon_config_id = occ.coupon_config_id
-- left join ods_vova_vtsf.ods_vova_kol_contact_email kce on kce.coupon_code = oi.coupon_code
left join tmp.tmp_zyzheng_0322_kcl kce on kce.coupon_code = oi.coupon_code
left join ods_vova_vts.ods_vova_shipping_method sm ON sm.sm_id = oi.sm_id
left join ods_vova_vts.ods_vova_region r on r.region_id = oi.country
where oi.coupon_code LIKE 'KOL%'
AND oi.pay_status >= 2


select
oi.pay_status,
oi.coupon_code AS fans_coupon_code,
from_unixtime(oc.coupon_ctime) as cpn_create_time,
oi.order_time,
oi.order_sn,
oi.email,
oi.order_amount,
-oi.bonus,
sm.sm_desc as shipping_method_name,
r.region_code
from
ods_vova_vts.ods_vova_order_info oi
inner join tmp.tmp_zyzheng_0322_kcl kce on kce.fans_coupon_code = oi.coupon_code
inner join ods_vova_vts.ods_vova_ok_coupon oc on oc.coupon_code = oi.coupon_code
inner join ods_vova_vts.ods_vova_ok_coupon_config occ on oc.coupon_config_id = occ.coupon_config_id
left join ods_vova_vts.ods_vova_shipping_method sm ON sm.sm_id = oi.sm_id
left join ods_vova_vts.ods_vova_region r on r.region_id = oi.country
where oi.pay_status >= 2
;


"



#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=20" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=dwb_vova_ad" \
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

