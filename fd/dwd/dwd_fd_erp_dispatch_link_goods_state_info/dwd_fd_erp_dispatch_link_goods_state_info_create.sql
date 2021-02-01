CREATE TABLE IF NOT EXISTS  dwd.dwd_fd_erp_dispatch_link_goods_state_info (
`record_time` string COMMENT '时间（中国）',
`no_receive_dispatch_num` bigint COMMENT '已分配未收货',
`no_sj_dispatch_num` bigint COMMENT '入库未上架(入库中)',
`onlocing_dispatch_num` bigint COMMENT '上架中',
`st_dispatch_num` bigint COMMENT '拣货下架中'
) COMMENT 'erp每日环节堆积量之商品状态统计信息表'
partitioned by (`pt` string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as PARQUET;