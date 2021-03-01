DROP TABLE ads.ads_vova_goods_sn_performance;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_goods_sn_performance
(
    goods_sn         string,
    datasource       string,
    platform         string,
    region_code      string,
    impressions      bigint,
    clicks           bigint,
    users            bigint,
    sales_order      bigint,
    gmv              decimal(15, 2),
    ctr              decimal(15, 4),
    rate             decimal(15, 4),
    gr               decimal(15, 4),
    gcr              decimal(15, 4),
    last_update_time TIMESTAMP,
    first_cat_name    string,
    second_cat_name   string,
    first_cat_id    bigint,
    second_cat_id   bigint,
    shop_price_amount decimal(15, 2),
    is_on_sale        bigint,
    brand_id          bigint,
    brand_name        string,
    third_cat_id      string,
    third_cat_name    string,
    fourth_cat_id     string,
    fourth_cat_name   string
) COMMENT 'ads_vova_goods_sn_performance' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
