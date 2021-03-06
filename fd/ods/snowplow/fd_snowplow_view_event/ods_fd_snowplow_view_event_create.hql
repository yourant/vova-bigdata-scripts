create  table if not exists ods_fd_snowplow.ods_fd_snowplow_view_event
(
    app_id               STRING,
    platform             STRING,
    project              STRING,
    platform_type        STRING,
    collector_ts         STRING,
    dvce_created_ts      STRING,
    dvce_sent_ts         STRING,
    etl_ts               STRING,
    derived_ts           STRING,
    os_tz                STRING,
    event_fingerprint    STRING,
    name_tracker         STRING,
    user_id              STRING,
    domain_userid        STRING,
    user_ipaddress       STRING,
    session_idx          BIGINT,
    session_id           STRING,
    useragent            STRING,
    dvce_type            STRING,
    dvce_ismobile        BOOLEAN,
    os_name              STRING,
    geo_country          STRING,
    geo_region           STRING,
    geo_city             STRING,
    geo_region_name      STRING,
    geo_timezone         STRING,
    raw_event_name       STRING,
    event_name           STRING,
    language             STRING,
    country              STRING,
    currency             STRING,
    page_code            STRING,
    user_unique_id       STRING,
    abtest               STRING,
    page_url             STRING,
    referrer_url         STRING,
    mkt_medium           STRING,
    mkt_source           STRING,
    mkt_term             STRING,
    mkt_content          STRING,
    mkt_campaign         STRING,
    mkt_clickid          STRING,
    mkt_network          STRING,
    user_fingerprint     STRING,
    br_name              STRING,
    br_lang              STRING,
    app_version          STRING,
    device_model         STRING,
    android_id           STRING,
    imei                 STRING,
    idfa                 STRING,
    idfv                 STRING,
    apple_id_fa          STRING,
    apple_id_fv          STRING,
    os_type              STRING,
    os_version           STRING,
    network_type         STRING,
    referrer_page_code   STRING,
    url_route_sn         STRING,
    url_virtual_goods_id STRING
) partitioned by (
    `pt` string,
    `hour` string
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    stored as parquet;

-- ---
-- set hive.exec.dynamic.partition.mode=nonstrict;
-- ---
--
-- INSERT OVERWRITE table ods_fd_snowplow.ods_fd_snowplow_view_event partition (pt, hour)
-- SELECT app_id,
--        platform,
--        project,
--        platform_type,
--        collector_ts,
--        dvce_created_ts,
--        dvce_sent_ts,
--        etl_ts,
--        derived_ts,
--        os_tz,
--        event_fingerprint,
--        name_tracker,
--        user_id,
--        domain_userid,
--        user_ipaddress,
--        session_idx,
--        session_id,
--        useragent,
--        dvce_type,
--        dvce_ismobile,
--        os_name,
--        geo_country,
--        geo_region,
--        geo_city,
--        geo_region_name,
--        geo_timezone,
--        raw_event_name,
--        event_name,
--        language,
--        country,
--        currency,
--        page_code,
--        user_unique_id,
--        abtest,
--        page_url,
--        referrer_url,
--        mkt_medium,
--        mkt_source,
--        mkt_term,
--        mkt_content,
--        mkt_campaign,
--        mkt_clickid,
--        mkt_network,
--        user_fingerprint,
--        br_name,
--        br_lang,
--        app_version,
--        device_model,
--        android_id,
--        imei,
--        idfa,
--        idfv,
--        apple_id_fa,
--        apple_id_fv,
--        os_type,
--        os_version,
--        network_type,
--        referrer_page_code,
--        url_route_sn,
--        url_virtual_goods_id,
--        pt,
--        hour
-- from ods_fd_snowplow.ods_fd_snowplow_all_event
-- where pt = ${hiveconf:pt_filter}
--   and event_name in ("screen_view","page_view")