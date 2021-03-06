#!/bin/bash
#指定日期和引擎
event_name="common_impression"
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
fi
sql="
INSERT OVERWRITE TABLE dwd.dwd_vova_log_impressions PARTITION(pt='${pt}', dp)
SELECT
/*+ REPARTITION(110) */
       datasource,
       event_fingerprint,
       event_name,
       platform,
       cast(collector_tstamp as timestamp) collector_tstamp,
       cast(dvce_created_tstamp as timestamp) dvce_created_tstamp,
       cast(derived_tstamp as timestamp) derived_tstamp,
       collector_tstamp collector_ts,
       dvce_created_tstamp dvce_created_ts,
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
       session_id,
       app_uri,
       list_type,
       element_id,
       element_name,
       element_type,
       element_position,
       extra,
       landing_page,
       imsi,
       null br_family,
       null br_version,
       case when datasource in ('airyclub','vova') then datasource
         else 'others'
         end dp
    from dwd.dwd_vova_log_impressions_history
WHERE pt='${pt}'
;
"

spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=20" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=dwd_vova_log_impressions_history" \
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
