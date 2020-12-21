DROP TABLE dim.dim_fn_goods;
CREATE TABLE IF NOT EXISTS dim.dim_fn_goods
(
    goods_id         bigint,
    virtual_goods_id bigint,
    datasource       string,
    original_source  string,
    source_id        bigint COMMENT '来源商品id,ad:goods_id,vo:virtual_goods_id',
    cat_id           bigint,
    is_on_sale       bigint,
    first_cat_id     bigint,
    first_cat_name   string,
    second_cat_id    bigint,
    second_cat_name  string,
    shop_price       decimal(13,2),
    img_original     string,
    commodity_id     bigint
) COMMENT '类目维度'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

