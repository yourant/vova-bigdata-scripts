CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_fd_ga_channel_campaign
(
    order_id     bigint COMMENT 'order_id',
    domain_userid string COMMENT 'domain_userid',
    pre_event_time string COMMENT 'pre_event_time',
    pre_ga_channel string COMMENT 'pre_ga_channel',
    pre_mkt_source string COMMENT 'pre_mkt_source',
    pre_campaign_name string COMMENT 'pre_campaign_name',
    pre_campaign_id string COMMENT 'pre_campaign_id',
    pre_adgroup_id string COMMENT 'pre_adgroup_id',
    pre_mkt_medium string COMMENT 'pre_mkt_medium',
    pre_mkt_term string COMMENT 'pre_mkt_term'
) COMMENT 'ads_fd_ga_channel_campaign'
PARTITIONED BY (pt string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;