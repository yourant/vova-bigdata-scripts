#!/bin/bash
#指定日期和引擎
event_name="screen_view"
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
fi
sql="
INSERT OVERWRITE TABLE dwd.dwd_vova_fact_log_screen_view PARTITION (pt='${pt}')
SELECT /*+ REPARTITION(20) */ event_fingerprint,
       datasource,
       event_name,
       platform,
       unix_timestamp(collector_tstamp)*1000 collector_tstamp,
       unix_timestamp(dvce_created_tstamp)*1000 dvce_created_tstamp,
       unix_timestamp(derived_tstamp)*1000 derived_tstamp,
       name_tracker,
       buyer_id,
       domain_userid,
       language,
       country,
       geo_country,
       geo_city,
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
       imsi
FROM dwd.dwd_vova_fact_log_screen_view_arc
WHERE pt='${pt}'
union all
SELECT /*+ REPARTITION(20) */ event_fingerprint,
       datasource,
       'screen_view' event_name,
       platform,
       unix_timestamp(collector_tstamp)*1000 collector_tstamp,
       unix_timestamp(dvce_created_tstamp)*1000 dvce_created_tstamp,
       unix_timestamp(derived_tstamp)*1000 derived_tstamp,
       name_tracker,
       buyer_id,
       domain_userid,
       language,
       country,
       geo_country,
       geo_city,
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
       imsi
FROM dwd.dwd_vova_fact_log_page_view_arc
WHERE pt='${pt}' and platform = 'mob' and os_type is not null
"
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" --conf "spark.sql.adaptive.shuffle.targetPostShuffleInputSize=128000000" --conf "spark.sql.adaptive.enabled=true" --conf "spark.app.name=dwd_vova_fact_log_screen_view" -e "$sql"
if [ $? -ne 0 ];then
  exit 1
fi
