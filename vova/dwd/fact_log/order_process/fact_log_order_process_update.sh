#!/bin/bash
#指定日期和引擎
event_name="order_process"
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
fi
sql="
INSERT OVERWRITE TABLE dwd.dwd_vova_log_order_process PARTITION (pt='${pt}', datasource)
SELECT /*+ REPARTITION(2) */ event_fingerprint,
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
       element_name,
       submit_result,
       virtual_goods_id,
       payment_method,
       null activity_code,
       null activity_detail,
       session_id,
       app_uri,
       landing_page,
       imsi,
       br_family,
       br_version,
       datasource
FROM dwd.dwd_vova_log_order_process_arc
WHERE pt='${pt}'"
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" --conf "spark.sql.adaptive.shuffle.targetPostShuffleInputSize=128000000" --conf "spark.sql.adaptive.enabled=true" --conf "spark.app.name=dwd_vova_log_order_process" -e "$sql"
if [ $? -ne 0 ];then
  exit 1
fi
