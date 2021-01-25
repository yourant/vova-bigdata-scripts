

CREATE TABLE IF NOT EXISTS  dwd.dwd_fd_erp_daily_goods_handle_info (
`report_date` string COMMENT '日期',
`receive_dispatch_num` bigint COMMENT '收货的工单件数',
`qc_dispatch_num` bigint COMMENT '质检的工单件数',
`sj_dispatch_num` bigint COMMENT '操作工单从框上架的工单件数',
`pk_dispatch_num` bigint COMMENT '操作拣货下架扫描的数据，多次拣货下架则记录多次'
) COMMENT 'erp每日商品操作信息表包括质检，收货，拣货，上架'
partitioned by (`pt` string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as PARQUET;