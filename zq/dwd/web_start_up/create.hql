drop table dwd.dwd_zq_fact_web_start_up;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd.dwd_zq_fact_web_start_up
(
    datasource     string comment '数据平台',
    domain_userid  string COMMENT '设备ID',
    buyer_id      bigint COMMENT '设备对应用户ID',
    platform      string COMMENT 'platform',
    region_code   string COMMENT 'geo_country',
    first_page_url   string COMMENT 'page_url',
    first_referrer   string COMMENT 'referrer',
    min_create_time  TIMESTAMP COMMENT '当日登录最小时间',
    max_create_time  TIMESTAMP COMMENT '当日登录最大时间'
) COMMENT 'fn_web用户访问表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

drop table dwd.dwd_zq_fact_original_channel_daily;
CREATE TABLE IF NOT EXISTS dwd.dwd_zq_fact_original_channel_daily
(
    datasource     string comment '数据平台',
    domain_userid  string COMMENT '设备ID',
    original_channel string COMMENT '来源',
    dvce_created_tstamp string COMMENT '设备对应用户ID'
) COMMENT 'fn_渠道来源' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

drop table dwd.dwd_zq_fact_original_channel;
CREATE TABLE IF NOT EXISTS dwd.dwd_zq_fact_original_channel
(
    datasource     string comment '数据平台',
    domain_userid  string COMMENT '设备ID',
    original_channel string COMMENT '来源',
    dvce_created_tstamp string COMMENT 'tstamp',
    pt             string COMMENT 'pt'
) COMMENT 'fn_渠道来源'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;





