create table if not exists ads.ads_fd_goods_age_group
(
    goods_id  bigint,
    age_group bigint,
    source string comment "数据来源"
)
    comment "商品年龄推算结果表"
    partitioned by (`pt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    stored as parquet;