drop table dwd.dwd_vova_fact_buyer_device_releation;
CREATE external TABLE IF NOT EXISTS dwd.dwd_vova_fact_buyer_device_releation
(
    buyer_id              bigint COMMENT '买家ID',
    datasource            string COMMENT '数据平台',
    device_id             string COMMENT '设备ID',
    app_version           string COMMENT 'app版本',
    app_region_code       string COMMENT 'country',
    region_code           string COMMENT 'geo_country',
    platform              string COMMENT 'ios|android'
) COMMENT '用户设备关系表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;