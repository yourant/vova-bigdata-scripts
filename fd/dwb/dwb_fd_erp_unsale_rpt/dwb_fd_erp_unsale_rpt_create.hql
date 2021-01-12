CREATE TABLE IF NOT EXISTS  dwb.dwb_fd_erp_unsale_rpt (
`unsale_level` string COMMENT '滞销程度',
`is_spring_stock` string COMMENT '是否春节备货1->是,0->否',
`unsale_rate` decimal(10, 6) COMMENT '滞销率',
`unsale_goods_num` bigint COMMENT '滞销件数',
`goods_number_total` bigint COMMENT '月销量',
`ws_goods_number_rate` decimal(10, 6) COMMENT '库存变化率'
) COMMENT 'erp滞销报表指标汇总'
partitioned by (`pt` string)
row format delimited fields terminated by '\t' lines terminated by '\n'
stored as PARQUET;