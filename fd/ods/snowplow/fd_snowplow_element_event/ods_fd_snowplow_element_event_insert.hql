INSERT OVERWRITE table ods_fd_snowplow.ods_fd_snowplow_element_event partition (pt="${pt}", hour="${hour}")
SELECT
/*+ REPARTITION(40) */
       app_id,
       platform,
       project,
       platform_type,
       collector_ts,
       dvce_created_ts,
       dvce_sent_ts,
       etl_ts,
       derived_ts,
       os_tz,
       event_fingerprint,
       name_tracker,
       user_id,
       domain_userid,
       user_ipaddress,
       session_idx,
       session_id,
       useragent,
       dvce_type,
       dvce_ismobile,
       os_name,
       geo_country,
       geo_region,
       geo_city,
       geo_region_name,
       geo_timezone,
       raw_event_name,
       event_name,
       language,
       country,
       currency,
       page_code,
       user_unique_id,
       abtest,
       page_url,
       referrer_url,
       mkt_medium,
       mkt_source,
       mkt_term,
       mkt_content,
       mkt_campaign,
       mkt_clickid,
       mkt_network,
       user_fingerprint,
       br_name,
       br_lang,
       app_version,
       device_model,
       android_id,
       imei,
       idfa,
       idfv,
       apple_id_fa,
       apple_id_fv,
       os_type,
       os_version,
       network_type,
       referrer_page_code,
       url_route_sn,
       url_virtual_goods_id,
       ee
from ods_fd_snowplow.ods_fd_snowplow_all_event
         LATERAL VIEW OUTER explode(element_event_struct) element_event as ee
where ${pt_filter}
  and event_name in ("common_click", "common_impression");