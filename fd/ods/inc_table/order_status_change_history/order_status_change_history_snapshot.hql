CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_order_status_change_history (
`id` bigint COMMENT '自增id',
`order_sn` string COMMENT '订单号',
`field_name` string COMMENT '字段名',
`old_value` int COMMENT '旧值',
`new_value` int COMMENT '新值',
`create_time` string COMMENT '发生时间'
) COMMENT '订单状态变更记录'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_order_status_change_history
select `(dt)?+.+` from ods_fd_vb.ods_fd_order_status_change_history_arc 
where dt = '${hiveconf:dt}';
