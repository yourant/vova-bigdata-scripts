CREATE TABLE IF NOT EXISTS `ods_fd_romeo.ods_fd_order_inv_reserved_detail_binlog_inc`(
  `order_inv_reserved_detail_id` string COMMENT '',
  `status` string COMMENT '',
  `order_id` bigint COMMENT '',
  `order_item_id` string COMMENT '',
  `goods_number` bigint COMMENT '',
  `product_id` string COMMENT '',
  `order_inv_reserved_id` string COMMENT '',
  `reserved_quantity` bigint COMMENT '',
  `reserved_time` timestamp COMMENT '',
  `status_id` string COMMENT '',
  `facility_id` string COMMENT '',
  `version` bigint COMMENT '乐观锁，版本号',
  `created_stamp` timestamp COMMENT '创建时间',
  `last_updated_stamp` timestamp COMMENT '修改时间',
  `event_type` string COMMENT 'binlog事件类型：update delete ...'
)COMMENT '来自kafka erp表每日增量数据'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;