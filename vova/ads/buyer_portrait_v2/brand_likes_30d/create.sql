drop table if exists ads.ads_vova_buyer_portrait_brand_likes_30d;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_buyer_portrait_brand_likes_30d
(
    `user_id`         bigint COMMENT 'd_买家id',
    `brand_id`         bigint COMMENT 'd_品牌id',
    `brand_name`       string COMMENT 'd_品牌name',
    `score`     decimal(10,6) COMMENt 'i_品牌偏好度'
) COMMENT '近30天商家类目等级数据'
    PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;