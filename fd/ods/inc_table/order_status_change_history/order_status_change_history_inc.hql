CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_order_status_change_history_inc (
`id` bigint COMMENT '自增id',
`order_sn` string COMMENT '订单号',
`field_name` string COMMENT '字段名',
`old_value` int COMMENT '旧值',
`new_value` int COMMENT '新值',
`create_time` string COMMENT '发生时间'
) COMMENT '订单状态变更记录'
PARTITIONED BY (dt STRING ) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


INSERT OVERWRITE TABLE ods_fd_vb.ods_fd_order_status_change_history_inc PARTITION (dt='${hiveconf:dt}')
select
    id,
    order_sn,
    field_name,
    old_value,
    new_value,
    create_time
from tmp.tmp_fd_order_status_change_history 
where dt = '${hiveconf:dt}';
