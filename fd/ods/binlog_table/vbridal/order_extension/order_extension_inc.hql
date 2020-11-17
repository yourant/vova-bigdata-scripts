CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_order_extension_inc
(
-- maxwell event data
    event_id STRING,
    event_table STRING,
    event_type STRING,
    event_commit BOOLEAN,
    event_date BIGINT,
-- now data
    id bigint,
    order_id bigint,
    ext_name string,
    ext_value string,
    is_delete bigint,
    last_update_time bigint COMMENT '最后更新时间'
) COMMENT 'kafka同步过来的数据库订单扩展表'
PARTITIONED BY (pt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite TABLE ods_fd_vb.ods_fd_order_extension_inc PARTITION (pt, hour)
SELECT  o_raw.xid AS event_id,
        o_raw.`table` AS event_table,
        o_raw.type AS event_type,
        cast(o_raw.`commit` AS BOOLEAN) AS event_commit,
        cast(o_raw.ts AS BIGINT) AS event_date,
        cast(o_raw.id AS INT) AS id,
        cast(o_raw.order_id AS INT) AS order_id,
        o_raw.ext_name AS ext_name,
        o_raw.ext_value AS ext_value,
        cast(o_raw.is_delete AS TINYINT) AS is_delete,
        cast(unix_timestamp(o_raw.last_update_time, 'yyyy-MM-dd HH:mm:ss') as BIGINT) AS last_update_time,
        pt,
        hour
FROM    tmp.tmp_fd_order_extension
LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type', 'kafka_old','id', 'order_id', 'ext_name', 'ext_value', 'is_delete', 'last_update_time') o_raw
AS `table`, ts, `commit`, xid, type, old, id, order_id, ext_name, ext_value, is_delete, last_update_time
WHERE pt >= '${hiveconf:pt}';
