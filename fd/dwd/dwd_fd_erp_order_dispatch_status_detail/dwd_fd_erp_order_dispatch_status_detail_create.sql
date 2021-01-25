CREATE TABLE dwd.dwd_fd_erp_order_dispatch_status_detail (
  `order_id` bigint ,
  `og_rec_id` bigint,
  `order_inv_reserved_detail_id` string,
  `goods_num` bigint,
  `reserved_num` bigint,
  `is_batch` boolean,
  `dispatch_list_id` string,
  `is_idle_stock` boolean,
  `dispatch_sn` string,
  `dispatch_status_id` string,
  `qc_status` string,
  `due_date` string,
  `is_receive` boolean,
  `is_qt` boolean,
  `multi_rk` boolean,
  `sj` boolean,
  `on_loc` boolean,
  `pk` boolean,
  `st` boolean,
  `multi_ck` boolean,
  `single_ck` boolean,
  `multi_ck_no_process` boolean,
  `single_ck_no_process` boolean
)COMMENT 'erp每日环节堆积量之商品状态统计信息明细表'
partitioned by (`pt` string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as PARQUET;

