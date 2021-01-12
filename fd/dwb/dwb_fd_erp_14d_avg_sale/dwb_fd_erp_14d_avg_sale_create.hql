CREATE TABLE IF NOT EXISTS dwb.dwb_fd_erp_14d_avg_sale (
`goods_id` bigint COMMENT '商品id',
`goods_sku` string COMMENT '商品sku',
`14d_avg_sale` decimal(10, 6) COMMENT '最近14天每天平均销量'
) COMMENT 'erp最近14天每天平均销量'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUET;