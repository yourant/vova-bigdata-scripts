drop table ads.ads_vova_img_enhance_d;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_img_enhance_d
(
    img_id                    string COMMENT '图片id',
    goods_id                  bigint COMMENT '商品id',
    img_url                   string COMMENT '图片链接',
    is_default                bigint COMMENT '是否首图'
) COMMENT '热搜词'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE location 's3://vova-computer-vision/product_data/vova_image_enhancement/src_data/';