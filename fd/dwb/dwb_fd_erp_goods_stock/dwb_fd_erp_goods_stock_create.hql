
CREATE TABLE IF NOT EXISTS dwb.dwb_fd_erp_goods_stock (
`goods_id` bigint COMMENT '商品id',
`stock_days` decimal(10, 2) COMMENT '备货时间'
) COMMENT 'erp备货天数指标数据'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUET;