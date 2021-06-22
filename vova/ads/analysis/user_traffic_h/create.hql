CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_user_traffic_analysis_h
(
    uv                bigint         comment 'uv',
    pv                bigint         comment 'pv'
) COMMENT '流量分时统计' PARTITIONED BY (pt String, hour String) STORED AS PARQUETFILE;