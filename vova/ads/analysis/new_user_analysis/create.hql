CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_new_user_analysis_h
(
    new_user_cnt       bigint comment '本日新下单用户',
    old_user_cnt       bigint comment '本日老下单用户'
) COMMENT '今日下单用户分布' PARTITIONED BY (pt String, hour String) STORED AS PARQUETFILE;