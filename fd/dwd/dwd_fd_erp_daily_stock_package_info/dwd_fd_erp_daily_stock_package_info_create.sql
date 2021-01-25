

CREATE TABLE IF NOT EXISTS  dwd.dwd_fd_erp_daily_stock_package_info (
`report_date` string COMMENT '日期',
`package_num` bigint COMMENT '打包数',
`pack_goods_num` bigint COMMENT '打包商品数量',
`ck_goods_num` bigint COMMENT '操作单件出库+多件出库的商品件数',
`rk_dispatch_num` bigint COMMENT '操作入库的工单件数'
) COMMENT 'erp每日出入库及打包数量信息表'
partitioned by (`pt` string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as PARQUET;