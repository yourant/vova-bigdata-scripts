DROP TABLE ads.ads_vova_img_search_source_d;
CREATE  external TABLE IF NOT EXISTS ads.ads_vova_img_search_source_d
(
    img_id                   bigint,
    sku_id                   bigint,
    goods_id                 bigint,
    img_url                  string,
    cat_id                   bigint,
    first_cat_id             bigint,
    first_cat_name           string,
    second_cat_id            bigint,
    second_cat_name          string,
    is_default               bigint,
    img_color                string,
    brand_id                 bigint
) COMMENT 'ads_vova_img_search_source_d' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location 's3://vova-computer-vision/product_data/vova_image_retrieval/src_data/';



DROP TABLE ads.ads_vova_img_search_source_his;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_img_search_source_his
(
    img_id                   bigint,
    sku_id                   bigint,
    goods_id                 bigint,
    img_url                  string,
    cat_id                   bigint,
    first_cat_id             bigint,
    first_cat_name           string,
    second_cat_id            bigint,
    second_cat_name          string,
    is_default               bigint,
    img_color                string,
    brand_id                 bigint
) COMMENT 'ads_vova_img_search_source_his' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;