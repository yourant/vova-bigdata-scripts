

CREATE TABLE IF NOT EXISTS  dwb.dwb_fd_erp_daily_workload (
`report_date` string COMMENT '日期',
`receive_dispatch_num` bigint COMMENT '收货的工单件数',
`qc_dispatch_num` bigint COMMENT '质检的工单件数',
`rk_dispatch_num` bigint COMMENT '操作入库的工单件数',
`sj_dispatch_num` bigint COMMENT '操作工单从框上架的工单件数',
`pk_dispatch_num` bigint COMMENT '操作拣货下架扫描的数据，多次拣货下架则记录多次',
`ck_goods_num` bigint COMMENT '操作单件出库+多件出库的商品件数',
`pack_goods_num` bigint COMMENT '打包',
`deliver_order_num` bigint COMMENT '发货的销售订单数',
`deliver_goods_num` bigint COMMENT '发货的销售订单中的商品总件数',
`reserved_unck_single_order_num` bigint COMMENT '预定成功未操作出库的单件订单',
`reserved_unck_multi_order_num` bigint COMMENT '预定成功未操作出库的多件订单',
`package_num` bigint COMMENT '打包数'
) COMMENT 'erp每日工作量报表'
partitioned by (`pt` string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as PARQUET;



