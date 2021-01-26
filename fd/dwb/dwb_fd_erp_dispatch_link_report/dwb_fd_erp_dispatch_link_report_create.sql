
CREATE TABLE IF NOT EXISTS  dwb.dwb_fd_erp_dispatch_link_report (
`record_time` string COMMENT '时间（中国）',
`undelivered_goods_num` bigint COMMENT '未发货的商品数量',
`due_dispatch_num` bigint COMMENT '工单超期数',
`no_dispatch_goods_num` bigint COMMENT '未创建工单',
`no_receive_dispatch_num` bigint COMMENT '已分配未收货',
`no_qt_dispatch_num` bigint COMMENT '已收货未质检',
`no_rk_dispatch_num` bigint COMMENT '已质检未入库',
`no_sj_dispatch_num` bigint COMMENT '入库未上架',
`onlocing_dispatch_num` bigint COMMENT '上架中',
`on_loc_dispatch_num` bigint COMMENT '在库位',
`st_dispatch_num` bigint COMMENT '拣货下架中',
`pk_dispatch_num` bigint COMMENT '分拣中',
`ck_dispatch_num` bigint COMMENT '出库中',
`ck_np_process_dispatch_num` bigint COMMENT '已出库未发货'
) COMMENT 'erp每日环节堆积量'
partitioned by (`pt` string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as PARQUET;