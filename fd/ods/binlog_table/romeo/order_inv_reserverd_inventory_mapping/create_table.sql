CREATE TABLE IF NOT EXISTS `ods_fd_romeo.ods_fd_order_inv_reserverd_inventory_mapping_binlog_inc`
(
	  `id` bigint COMMENT '',
	  `order_inv_reserved_detail_id` string COMMENT '',
	  `inventory_item_id` string COMMENT '',
	  `quantity` bigint COMMENT '数量',
	  `created_stamp` timestamp COMMENT '创建时间',
	  `last_updated_stamp` timestamp COMMENT '修改时间',
	  `event_type` string COMMENT 'binlog事件类型：update delete ...'
)COMMENT '来自kafka erp表每日增量数据'
PARTITIONED BY (`pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_order_inv_reserverd_inventory_mapping_arc (
    `id` bigint COMMENT '',
    `order_inv_reserved_detail_id` string COMMENT '',
    `inventory_item_id` string COMMENT '',
    `quantity` bigint COMMENT '数量',
    `created_stamp` timestamp COMMENT '创建时间',
    `last_updated_stamp` timestamp COMMENT '修改时间'
) COMMENT '来自kafka erp订单每日增量数据'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_order_inv_reserverd_inventory_mapping (
    `id` bigint COMMENT '',
    `order_inv_reserved_detail_id` string COMMENT '',
    `inventory_item_id` string COMMENT '',
    `quantity` bigint COMMENT '数量',
    `created_stamp` timestamp COMMENT '创建时间',
    `last_updated_stamp` timestamp COMMENT '修改时间'
) COMMENT '来自对应arc表的数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;



