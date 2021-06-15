#!/bin/bash
#指定日期和引擎
cur_date=$1
pre_hour=$2
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=$(date "+%Y-%m-%d")
fi
if [ ! -n "$2" ];then
pre_hour=$(date "+%H")
fi

echo "time:${cur_date} ${pre_hour}"

sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ads.ads_vova_funnel_analysis_h partition(pt,hour)
select
count(distinct device_id) dau,
count(distinct pd_device_id) pd_uv,
count(distinct cart_success_device_id) cart_success_uv,
count(distinct checkout_device_id) checkout_uv,
count(distinct payment_device_id)  payment_uv,
count(distinct payment_confirm_device_id) payment_confirm_uv,
'${cur_date}' as pt,
'${pre_hour}' as hour
from
(select
case when t.event_name ='screen_view' THEN t.device_id end device_id,
case when t.page_code ='product_detail' and view_type='show' then t.device_id end pd_device_id,
case when t.element_name ='pdAddToCartSuccess' then t.device_id end cart_success_device_id,
case when t.page_code like '%checkout%' and view_type='show' THEN t.device_id  end checkout_device_id,
case when t.page_code ='payment' and view_type='show' then t.device_id end payment_device_id,
case when t.element_name ='payment_confirm' then t.device_id end payment_confirm_device_id
from
(
select event_name, hour(collector_ts) hour,datasource,os_type,device_id,page_code,null element_name ,view_type,app_version,geo_country from dwd.dwd_vova_log_screen_view_arc where pt='${cur_date}' and hour<=${pre_hour}
union all
select 'screen_view' event_name, hour(collector_ts) hour,datasource,os_type,device_id,page_code,null element_name ,view_type,app_version,geo_country from dwd.dwd_vova_log_page_view_arc where pt='${cur_date}' and hour<=${pre_hour}
union all
select event_name,hour(collector_ts) hour,datasource,os_type,device_id,null page_code, element_name ,null view_type,app_version,geo_country from dwd.dwd_vova_log_common_click_arc where pt='${cur_date}' and hour<=${pre_hour}
union all
select event_name,hour(collector_ts) hour,datasource,os_type,device_id,null page_code, element_name ,null view_type,app_version,geo_country from dwd.dwd_vova_log_click_arc where pt='${cur_date}' and event_type='normal' and hour<=${pre_hour}
union all
select event_name,hour(collector_ts) hour,datasource,os_type,device_id,null page_code, element_name ,null view_type,app_version,geo_country from dwd.dwd_vova_log_data_arc where pt='${cur_date}'  and element_name ='pdAddToCartSuccess' and hour<=${pre_hour}
) t)

"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_order_gmv_analysis_h" \
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
