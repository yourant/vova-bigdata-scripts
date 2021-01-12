CREATE TABLE IF NOT EXISTS dwb.dwb_fd_erp_unreserve_order (
`goods_id` bigint COMMENT '商品id',
`goods_sku` string COMMENT '商品sku',
`goods_number` bigint COMMENT '未预定上的订单需求数'
) COMMENT 'erp未预定上的订单需求数'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUET;