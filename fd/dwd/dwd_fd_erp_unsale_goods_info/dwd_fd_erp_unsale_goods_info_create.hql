CREATE TABLE IF NOT EXISTS dwd.dwd_fd_erp_unsale_goods_info (
`goods_id` bigint COMMENT '商品id',
`goods_sku` string COMMENT '商品sku',
`14d_avg_sale` decimal(15, 4) COMMENT '最近14天每天平均销量',
`goods_number_month` bigint COMMENT '最近一个月销量',
`stock_days` decimal(15, 4) COMMENT '备货时间',
`reserve_num` bigint COMMENT '可预订库存量',
`goods_number` bigint COMMENT '未预定上的订单需求数'
) COMMENT 'erp滞销商品信息数据'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS PARQUET;