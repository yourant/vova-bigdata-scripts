#!/bin/bash
#指定日期和引擎
event_name="goods_click"
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
fi
sql="
INSERT OVERWRITE TABLE dwd.dwd_vova_log_goods_click PARTITION (pt='${pt}', dp)
SELECT
/*+ REPARTITION(5) */
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
       list_uri,
       list_type,
       virtual_goods_id,
       absolute_position,
       element_url,
       recall_pool,
       activity_code,
       activity_detail,
       session_id,
       app_uri,
       element_type,
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
       null view_type,
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
       list_uri,
       list_type,
       virtual_goods_id,
       absolute_position,
       element_url,
       recall_pool,
       null activity_code,
       null activity_detail,
       session_id,
       app_uri,
       element_type,
       landing_page,
       imsi,
       br_family,
       br_version,
       datasource
FROM dwd.dwd_vova_log_goods_click_arc
WHERE pt='${pt}'
union all
SELECT event_fingerprint,
       'goods_click' event_name,
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
       null view_type,
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
       list_uri,
       list_type,
       cast(element_id as bigint) virtual_goods_id,
       element_position absolute_position,
       null element_url,
       recall_pool,
       null activity_code,
       null activity_detail,
       session_id,
       app_uri,
       element_type,
       landing_page,
       imsi,
       br_family,
       br_version,
       datasource
FROM dwd.dwd_vova_log_click_arc
WHERE ((pt='${pt}'and date(collector_ts)='${pt}' ) or (pt=date_sub('${pt}',1) and hour ='23' and date(collector_ts)='${pt}') or (pt=date_add('${pt}',1) and hour ='00' and date(collector_ts)='${pt}')) and event_type='goods'
)
;
"

spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=20" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=dwd_vova_log_goods_click" \
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
#hive -e "$sql"
if [ $? -ne 0 ];then
  exit 1
fi
