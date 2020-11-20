create table if not exists ods_fd_snowplow.ods_fd_snowplow_all_event
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
    url_virtual_goods_id String,
    goods_event_struct    array<struct<list_uri : String, list_type : String, virtual_goods_id : String, picture
                                      : String, page_position : bigint, absolute_position : bigint, page_size : bigint,
                                      page_no
                                      : bigint, element_name : String, extra : String>>,
    element_event_struct array<struct<list_uri : String, list_type : String, element_name : String, element_url
                                      : String, element_content : String, element_id : String, element_type : String,
                                      picture : String, absolute_position : BIGINT, extra : String>>,
    data_event_struct    struct<element_name : String, extra : String>,
    ecommerce_action     struct<id : String, affiliation : String, option : String, list : String, revenue : Double,
                                step : BIGINT>,
    ecommerce_product    array<struct<id : String, name : String, brand : String, category : String, coupon : String,
                                      position : BIGINT, price : Double, quantity : BIGINT, variant : String>>
) partitioned by (
    `pt` string,
    `hour` int
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    stored as parquet;

---
set hive.exec.dynamic.partition.mode=nonstrict;
---

INSERT OVERWRITE table ods_fd_snowplow.ods_fd_snowplow_all_event partition (pt, hour)
SELECT common_struct.app_id,
       common_struct.platform,
       common_struct.project,
       common_struct.platform_type,
       common_struct.collector_ts,
       common_struct.dvce_created_ts,
       common_struct.dvce_sent_ts,
       common_struct.etl_ts,
       common_struct.derived_ts,
       common_struct.os_tz,
       common_struct.event_fingerprint,
       common_struct.name_tracker,
       common_struct.user_id,
       common_struct.domain_userid,
       common_struct.user_ipaddress,
       common_struct.session_idx,
       common_struct.session_id,
       common_struct.useragent,
       common_struct.dvce_type,
       common_struct.dvce_ismobile,
       common_struct.os_name,
       common_struct.geo_country,
       common_struct.geo_region,
       common_struct.geo_city,
       common_struct.geo_region_name,
       common_struct.geo_timezone,
       common_struct.raw_event_name,
       common_struct.event_name,
       common_struct.language,
       common_struct.country,
       common_struct.currency,
       common_struct.page_code,
       common_struct.user_unique_id,
       common_struct.abtest,
       common_struct.page_url,
       common_struct.referrer_url,
       common_struct.mkt_medium,
       common_struct.mkt_source,
       common_struct.mkt_term,
       common_struct.mkt_content,
       common_struct.mkt_campaign,
       common_struct.mkt_clickid,
       common_struct.mkt_network,
       common_struct.user_fingerprint,
       common_struct.br_name,
       common_struct.br_lang,
       common_struct.app_version,
       common_struct.device_model,
       common_struct.android_id,
       common_struct.imei,
       common_struct.idfa,
       common_struct.idfv,
       common_struct.apple_id_fa,
       common_struct.apple_id_fv,
       common_struct.os_type,
       common_struct.os_version,
       common_struct.network_type,
       common_struct.referrer_page_code,
       common_struct.url_route_sn,
       common_struct.url_virtual_goods_id,
       goods_event_struct,
       element_event_struct,
       data_event_struct,
       ecommerce_action,
       ecommerce_product,
       date(common_struct.collector_ts),
       hour(common_struct.collector_ts)
from (select common_struct,
             goods_event_struct,
             element_event_struct,
             data_event_struct,
             ecommerce_action,
             ecommerce_product,
             row_number()
                     over (partition by common_struct.event_fingerprint order by common_struct.collector_ts asc ) as row_num
      from pdb.pdb_fd_snowplow_all_event
      where ${hiveconf:pt_filter}
        and common_struct.collector_ts >= "${hiveconf:start}"
        and common_struct.collector_ts < "${hiveconf:end}"
        and common_struct.app_id is not null) tmp
where row_num = 1;