CREATE TABLE IF NOT EXISTS  dwd.dwd_fd_erp_daily_order_goods_nums_info (
`report_date` string COMMENT '日期',
`deliver_order_num` bigint COMMENT '发货的销售订单数',
`deliver_goods_num` bigint COMMENT '发货的销售订单中的商品总件数',
`reserved_unck_single_order_num` bigint COMMENT '预定成功未操作出库的单件订单',
`reserved_unck_multi_order_num` bigint COMMENT '预定成功未操作出库的多件订单'
) COMMENT 'erp每日发货订单及商品预定数量信息表'
partitioned by (`pt` string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as PARQUET;