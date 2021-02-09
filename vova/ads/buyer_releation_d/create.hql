drop table ads.ads_vova_buyer_releation_d;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_buyer_releation_d
(
    user_id                bigint COMMENT '用户id',
    app_version            string COMMENT '当前app版本号',
    last_update_time       timestamp COMMENT ''
) COMMENT ''
 PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;