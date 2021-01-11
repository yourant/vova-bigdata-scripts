#!/bin/bash
#指定日期和引擎
event_name="common_click"
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
fi
sql="
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE dwd.dwd_vova_log_common_click PARTITION (pt='${pt}', dp)
select
/*+ REPARTITION(25) */
datasource,
event_fingerprint,
event_name,
platform,
collector_tstamp,
dvce_created_tstamp,
derived_tstamp,
cast(collector_tstamp/1000 as timestamp) collector_ts,
cast(dvce_created_tstamp/1000 as timestamp) dvce_created_ts,
name_tracker,
buyer_id,
domain_userid,
language,
country,
geo_country,
geo_city,
null geo_region,
null geo_latitude,
null geo_longitude,
null geo_region_name,
null geo_timezone,
currency,
page_code,
gender,
page_url,
account_class,
channel_type,
view_type,
app_version,
device_model,
device_id,
referrer,
organic_idfv,
advertising_id,
advertising_id_sp,
test_info,
media_source,
sys_lang,
sys_country,
vpn,
email,
latlng,
root,
is_tablet,
os_type,
os_version,
ip,
element_name,
element_url,
element_content,
list_uri,
list_name,
element_id,
element_type,
element_position,
activity_code,
activity_detail,
session_id,
app_uri,
landing_page,
imsi,
null br_family,
null br_version,
case when datasource in ('airyclub','vova') then datasource
    else 'others'
    end dp
from
dwd.dwd_vova_log_common_click_history
where pt='$pt'
;
"


spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=20" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=dwd_vova_log_common_click_his" \
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

