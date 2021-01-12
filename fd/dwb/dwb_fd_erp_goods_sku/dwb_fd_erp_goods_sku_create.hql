
CREATE TABLE IF NOT EXISTS dwb.dwb_fd_erp_goods_sku (
`goods_id` bigint COMMENT '商品id',
`goods_sku` string COMMENT '商品sku'
) COMMENT 'erp商品id和SKU'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUET;