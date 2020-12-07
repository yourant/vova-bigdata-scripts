create table dwb.dwb_fd_on_sale_goods_price
(
    goods_id           BIGINT,
    virtual_goods_id   BIGINT,
    project_name       STRING,
    cat_id             BIGINT,
    first_cat_id      BIGINT,
    first_cat_name    STRING,
    shop_price_usd     DECIMAL(15, 4),
    purchase_price_rmb DECIMAL(15, 4)
) partitioned by (
    `pt` string
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    stored as parquet;
