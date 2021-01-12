CREATE TABLE IF NOT EXISTS  dwb.dwb_fd_erp_unsale_detail (
`unsale_level` string COMMENT '滞销程度',
`goods_id` bigint COMMENT '商品id',
`goods_sku` string COMMENT '商品SKU',
`stock_days` decimal(10, 2) COMMENT '备货天数',
`14d_avg_sale` decimal(10, 6) COMMENT '最近14天每天平均销量',
`goods_number` bigint COMMENT '未预定上的订单需求数',
`reserve_num` bigint COMMENT '可预订库存',
`goods_number_month` bigint COMMENT '当月销量',
`can_sale_days` decimal(10, 2) COMMENT '可售天数',
`back_days` decimal(10, 2) COMMENT 'max(30,备货天数)',
`unsale_goods_num` decimal(10, 2) COMMENT 'max(30,备货天数)',
`is_spring_stock` int COMMENT '是否春节备货1->是,0->否'
) COMMENT 'romeo组织的配置表'
partitioned by (`pt` string)
row format delimited fields terminated by '\t' lines terminated by '\n'
stored as PARQUET;