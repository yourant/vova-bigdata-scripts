create table if not exists dwd.dwd_fd_goods_purchase_shop_price
(
    goods_id           BIGINT,
    virtual_goods_id   BIGINT,
    project_name       STRING,
    cat_id             BIGINT,
    shop_price_usd     DECIMAL(15, 4),
    purchase_price_rmb DECIMAL(15, 4),
    is_on_sale         BOOLEAN
) comment "每天商品采购价售价的快照"
    partitioned by (`pt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    stored as parquet;