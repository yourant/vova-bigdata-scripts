CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_order_status_change_history_inc (
    id INT,
    order_sn STRING,
    field_name STRING,
    old_value BIGINT,
    new_value BIGINT,
    create_time timestamp COMMENT '最后更新时间'
) COMMENT 'kafka同步过来的数据库订单状态变化表'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_order_status_change_history_arc (
    id INT,
    order_sn STRING,
    field_name STRING,
    old_value BIGINT,
    new_value BIGINT,
    create_time timestamp COMMENT '最后更新时间'
) COMMENT 'kafka同步过来的数据库订单状态变化表'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

CREATE EXTERNAL TABLE IF NOT EXISTS ods_fd_vb.ods_fd_order_status_change_history(
    id INT,
    order_sn STRING,
    field_name STRING,
    old_value BIGINT,
    new_value BIGINT,
    create_time timestamp COMMENT '最后更新时间'
) COMMENT 'kafka同步过来的数据库订单状态变化表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

