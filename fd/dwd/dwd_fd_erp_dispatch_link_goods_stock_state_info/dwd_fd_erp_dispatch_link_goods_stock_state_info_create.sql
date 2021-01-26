CREATE TABLE IF NOT EXISTS  dwd.dwd_fd_erp_dispatch_link_goods_stock_state_info (
`record_time` string COMMENT '时间（中国）',
`no_qt_dispatch_num` bigint COMMENT '已收货未质检',
`no_rk_dispatch_num` bigint COMMENT '已质检未入库',
`on_loc_dispatch_num` bigint COMMENT '在库位',
`ck_np_process_dispatch_num` bigint COMMENT '已出库未发货'
) COMMENT 'erp每日环节堆积量之库存状态信息统计表'
partitioned by (`pt` string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as PARQUET;