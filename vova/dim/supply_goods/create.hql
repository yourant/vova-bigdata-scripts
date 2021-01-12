DROP TABLE dim.dim_vova_supply_goods;
CREATE EXTERNAL TABLE IF NOT EXISTS dim.dim_vova_supply_goods
(
    goods_id        bigint COMMENT 'vova.goods_id',
    commodity_id    string COMMENT 'supply.commodity_id',
    product_id      string COMMENT 'supply.goods_id',
    first_cat_name  string COMMENT '供应链商品pdd一级分类',
    second_cat_name string COMMENT '供应链商品pdd一级分类',
    three_cat_name  string COMMENT '供应链商品pdd一级分类'
) COMMENT 'vova商品pdd数据维表' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
