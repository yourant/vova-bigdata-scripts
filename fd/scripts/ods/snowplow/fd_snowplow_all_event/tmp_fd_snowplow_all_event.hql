create external table if not exists tmp.tmp_fd_snowplow_all_event
(
    common_struct        struct<app_id :STRING, platform :STRING, project :STRING, platform_type :STRING, collector_ts
                                :STRING, dvce_created_ts :STRING, dvce_sent_ts :STRING, etl_ts :STRING, derived_ts
                                :STRING, os_tz
                                :STRING, event_fingerprint :STRING, name_tracker :STRING, user_id :STRING, domain_userid
                                :STRING, user_ipaddress :STRING, session_idx :BIGINT, session_id :STRING, useragent
                                :STRING, dvce_type :STRING, dvce_ismobile :BOOLEAN, os_name :STRING, geo_country
                                :STRING, geo_region
                                :STRING, geo_city :STRING, geo_region_name :STRING, geo_timezone :STRING, raw_event_name
                                :STRING, event_name :STRING, language :STRING, country :STRING, currency :STRING,
                                page_code
                                :STRING, user_unique_id :STRING, abtest :STRING, page_url :STRING, referrer_url :STRING,
                                mkt_medium :STRING, mkt_source :STRING, mkt_term :STRING, mkt_content :STRING,
                                mkt_campaign
                                :STRING, mkt_clickid :STRING, mkt_network :STRING, user_fingerprint :STRING, br_name
                                :STRING, br_lang :STRING, app_version :STRING, device_model :STRING, android_id :STRING,
                                imei :STRING, idfa :STRING, idfv :STRING, apple_id_fa :STRING, apple_id_fv :STRING,
                                os_type :STRING, os_version :STRING, network_type :STRING, referrer_page_code :String,url_virtual_goods_id
                                :bigint,url_route_sn :bigint>,
    goods_event_struct   array<struct<list_uri : String, list_type : String, virtual_goods_id : String, picture
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
) PARTITIONED BY (
    `dt` string,
    `hour` string)
    ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
    stored as textfile
    location '${hiveconf:flume_path}/fd/snowplow/snowplow_all_event';

MSCK REPAIR TABLE tmp.tmp_fd_snowplow_all_event;
