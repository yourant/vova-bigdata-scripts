CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_order_inv_reserved_inc (
    -- maxwell event data
    event_id STRING,
    event_table STRING,
    event_type STRING,
    event_commit BOOLEAN,
    event_date BIGINT,
    -- now data
    order_inv_reserved_id string,
    `version`             string,
    status                string,
    order_id              bigint,
    facility_id           string,
    container_id          string,
    party_id              string,
    reserved_time         bigint,
    order_time            bigint
) COMMENT '来自kafka erp表每日增量数据'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_order_inv_reserved_inc  PARTITION (dt='${hiveconf:dt}',hour)
select 
    o_raw.xid AS event_id,
    o_raw.`table` AS event_table,
    o_raw.type AS event_type,
    cast(o_raw.`commit` AS BOOLEAN) AS event_commit,
    cast(o_raw.ts AS BIGINT) AS event_date,
    o_raw.order_inv_reserved_id,
    o_raw.`version`,
    o_raw.status,
    o_raw.order_id,
    o_raw.facility_id,
    o_raw.container_id,
    o_raw.party_id,
    /* timezone Asia/Shanghai in mysql ecshop database, convert to UTC */
    if(o_raw.reserved_time != "0000-00-00 00:00:00", unix_timestamp(to_utc_timestamp(o_raw.reserved_time, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) as reserved_time,
    /* timezone Asia/Shanghai in mysql ecshop database, convert to UTC */
    if(o_raw.order_time != "0000-00-00 00:00:00", unix_timestamp(to_utc_timestamp(o_raw.order_time, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) as order_time,
    hour as hour
from tmp.tmp_fd_romeo_order_inv_reserved
LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'order_inv_reserved_id', 'version', 'status', 'order_id', 'facility_id', 'container_id', 'party_id', 'reserved_time', 'order_time') o_raw
AS `table`, ts, `commit`, xid, type, old, order_inv_reserved_id,`version`,status,order_id,facility_id,container_id,party_id,reserved_time,order_time
where dt = '${hiveconf:dt}';
