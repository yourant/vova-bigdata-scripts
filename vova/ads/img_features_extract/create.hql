drop table ads.ads_vova_img_features_extract_d;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_img_features_extract_d
(
    img_id                    string COMMENT '图片id',
    goods_id                  bigint COMMENT '商品id',
    img_url                   string COMMENT '图片链接'
) COMMENT '基础图片'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE location 's3://vova-computer-vision/product_data/vova_img_features_extract/src_data/';

