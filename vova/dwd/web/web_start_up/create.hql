drop table dwd.dwd_vova_fact_original_channel_daily;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd.dwd_vova_fact_original_channel_daily
(
    datasource     string comment '数据平台',
    domain_userid  string COMMENT '设备ID',
    region_code    string COMMENT 'region_code',
    platform       string COMMENT 'platform',
    original_channel string COMMENT '来源',
    dvce_created_ts string COMMENT '设备对应用户ID',
    buyer_id        bigint COMMENT 'buyer_id'
) COMMENT 'dwd_vova_fact_original_channel_daily' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

drop table dwd.dwd_vova_fact_original_channel;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd.dwd_vova_fact_original_channel
(
    datasource     string comment '数据平台',
    domain_userid  string COMMENT '设备ID',
    region_code    string COMMENT 'region_code',
    platform       string COMMENT 'platform',
    original_channel string COMMENT '来源',
    dvce_created_ts string COMMENT 'dvce_created_ts',
    buyer_id        bigint COMMENT 'buyer_id',
    activate_date  string COMMENT '首次激活时间'
) COMMENT 'dwd_vova_fact_original_channel'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;





