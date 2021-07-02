CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_fd_base_snowplow
(
    event_fingerprint       string,
    dt                      string,
    country                 string,
    domain_userid           string,
    useragent               string,

    element_name            string,
    adgroup_id              string,
    ads_type                string,
    campaign_id             string,
    adset_id                string
) COMMENT '营销需要的打点数据'
    PARTITIONED BY (pt string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS PARQUETFILE;