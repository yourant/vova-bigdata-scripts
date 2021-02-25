CREATE TABLE IF NOT EXISTS `ods_fd_romeo.ods_fd_order_inv_reserved_binlog_inc`(
  `order_inv_reserved_id` string COMMENT '',
  `version` string COMMENT '',
  `status` string COMMENT '',
  `order_id` bigint COMMENT '',
  `facility_id` string COMMENT '',
  `container_id` string COMMENT '',
  `party_id` string COMMENT '',
  `reserved_time` timestamp COMMENT '',
  `delivery_time` timestamp COMMENT '交期',
  `order_time` timestamp COMMENT '',
  `version2` bigint COMMENT '乐观锁，版本号',
  `created_stamp` timestamp COMMENT '创建时间',
  `last_updated_stamp` timestamp COMMENT '修改时间',
  `event_type` string COMMENT 'binlog事件类型：update delete ...'
)COMMENT '来自kafka erp表每日增量数据'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;