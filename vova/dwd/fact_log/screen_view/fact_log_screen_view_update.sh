#!/bin/bash
#指定日期和引擎
event_name="screen_view"
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
fi
sql="
INSERT OVERWRITE TABLE dwd.dwd_vova_log_screen_view PARTITION (pt='${pt}', dp)
SELECT
/*+ REPARTITION(45) */
       datasource,
       event_fingerprint,
       event_name,
       platform,
       collector_tstamp,
       dvce_created_tstamp,
       derived_tstamp,
       collector_ts,
       dvce_created_ts,
       name_tracker,
       buyer_id,
       domain_userid,
       language,
       country,
       geo_country,
       geo_city,
       geo_region,
       geo_latitude,
       geo_longitude,
       geo_region_name,
       geo_timezone,
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
       activity_code,
       activity_detail,
       session_id,
       virtual_goods_id,
       app_uri,
       landing_page,
       imsi,
       br_family,
       br_version,
       case when datasource in ('airyclub','vova') then datasource
         else 'others'
         end dp
FROM
(
SELECT event_fingerprint,
       event_name,
       platform,
       collector_tstamp,
       dvce_created_tstamp,
       derived_tstamp,
       collector_ts,
       dvce_created_ts,
       name_tracker,
       buyer_id,
       domain_userid,
       language,
       country,
       geo_country,
       geo_city,
       geo_region,
       geo_latitude,
       geo_longitude,
       geo_region_name,
       geo_timezone,
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
       null activity_code,
       null activity_detail,
       session_id,
       virtual_goods_id,
       app_uri,
       landing_page,
       imsi,
       br_family,
       br_version,
       datasource
FROM dwd.dwd_vova_log_screen_view_arc
WHERE pt='${pt}'
union all
SELECT event_fingerprint,
       'screen_view' event_name,
       platform,
       collector_tstamp,
       dvce_created_tstamp,
       derived_tstamp,
       collector_ts,
       dvce_created_ts,
       name_tracker,
       buyer_id,
       domain_userid,
       language,
       country,
       geo_country,
       geo_city,
       geo_region,
       geo_latitude,
       geo_longitude,
       geo_region_name,
       geo_timezone,
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
       null activity_code,
       null activity_detail,
       session_id,
       virtual_goods_id,
       app_uri,
       landing_page,
       imsi,
       br_family,
       br_version,
       datasource
FROM dwd.dwd_vova_log_page_view_arc
WHERE ((pt='${pt}'and date(collector_ts)='${pt}' ) or (pt=date_sub('${pt}',1) and hour ='23' and date(collector_ts)='${pt}') or (pt=date_add('${pt}',1) and hour ='00' and date(collector_ts)='${pt}')) and platform = 'mob' and os_type is not null
)
;
"
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=20" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=dwd_vova_log_screen_view" \
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
