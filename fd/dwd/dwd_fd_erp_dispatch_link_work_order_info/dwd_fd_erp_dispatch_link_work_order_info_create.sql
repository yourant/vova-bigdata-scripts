CREATE TABLE IF NOT EXISTS  dwd.dwd_fd_erp_dispatch_link_work_order_info (
`record_time` string COMMENT '时间（中国）',
`due_dispatch_num` bigint COMMENT '工单超期数',
`no_dispatch_goods_num` bigint COMMENT '未创建工单'
) COMMENT 'erp每日环节堆积量之工单状态统计信息表'
partitioned by (`pt` string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as PARQUET;