DROP TABLE ads.ads_vova_goods_performance_page;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_goods_performance_page
(
    datasource       string COMMENT '项目',
    page_code        string COMMENT '页面',
    platform        string COMMENT  'platform',
    region_code        string COMMENT  'region_code',
    goods_id         bigint COMMENT '商品id',
    goods_sn         string COMMENT 'goods_sn',
    sales_order      bigint COMMENT '销售订单数',
    gmv              decimal(15, 2) COMMENT 'gmv',
    impressions      bigint COMMENT '曝光数',
    clicks           bigint COMMENT '点击数',
    users            bigint COMMENT '点击uv',
    ctr              decimal(15, 4) COMMENT 'clicks / impressions',
    rate             decimal(15, 4) COMMENT 'sales_order / users',
    gr               decimal(15, 4) COMMENT 'gmv / users',
    gcr              decimal(15, 4) COMMENT 'gmv / users * clicks / impressions',
    last_update_time TIMESTAMP,
    first_cat_name    string,
    second_cat_name   string,
    first_cat_id    bigint,
    second_cat_id   bigint,
    shop_price_amount decimal(15, 2),
    is_on_sale        bigint,
    brand_id          bigint,
    brand_name        string,
    mct_name          string,
    mct_id            bigint,
    third_cat_id      string,
    third_cat_name    string,
    fourth_cat_id     string,
    fourth_cat_name   string
) COMMENT '商品表现-页面' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


