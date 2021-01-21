drop table dim.dim_zq_goods;
CREATE EXTERNAL TABLE IF NOT EXISTS dim.dim_zq_goods
(
    goods_id         bigint,
    virtual_goods_id bigint,
    datasource       string,
    original_source  string,
    source_id        bigint COMMENT '来源商品id,ad:goods_id,vo:virtual_goods_id',
    cat_id           bigint,
    cat_name         string,
    is_on_sale       bigint,
    first_cat_id     bigint,
    first_cat_name   string,
    second_cat_id    bigint,
    second_cat_name  string,
    shop_price       decimal(13,2),
    img_original     string,
    commodity_id     string,
    domain_group     string COMMENT '组织'
) COMMENT '站群商品维表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;




