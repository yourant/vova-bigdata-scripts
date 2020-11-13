CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_order_inv_reserved_detail_inc (
    -- maxwell event data
    event_id STRING,
    event_table STRING,
    event_type STRING,
    event_commit BOOLEAN,
    event_date BIGINT,
    -- now data
    order_inv_reserved_detail_id string,
    status                       string,
    order_id                     bigint,
    order_item_id                bigint,
    goods_number                 bigint,
    product_id                   bigint,
    order_inv_reserved_id        string,
    reserved_quantity            bigint,
    reserved_time                bigint,
    status_id                    string,
    facility_id                  bigint,
    version                      bigint,
    created_stamp                bigint,
    last_updated_stamp           bigint
) COMMENT '来自kafka erp订单每日增量数据'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY")
;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_order_inv_reserved_detail_inc  PARTITION (dt='${hiveconf:dt}',hour)
select 
    o_raw.xid AS event_id,
    o_raw.`table` AS event_table,
    o_raw.type AS event_type,
    cast(o_raw.`commit` AS BOOLEAN) AS event_commit,
    cast(o_raw.ts AS BIGINT) AS event_date,
    o_raw.order_inv_reserved_detail_id,
    o_raw.status,
    o_raw.order_id,
    o_raw.order_item_id,
    o_raw.goods_number,
    o_raw.product_id,
    o_raw.order_inv_reserved_id,
    o_raw.reserved_quantity,
    if(o_raw.reserved_time != "0000-00-00 00:00:00" or o_raw.reserved_time is not null,
        unix_timestamp(to_utc_timestamp(o_raw.reserved_time, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS reserved_time,
    o_raw.status_id,
    o_raw.facility_id,
    version,
    if(o_raw.created_stamp != "0000-00-00 00:00:00" or o_raw.created_stamp is not null,
        unix_timestamp(to_utc_timestamp(o_raw.created_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS created_stamp,
    if(o_raw.last_updated_stamp != "0000-00-00 00:00:00" or o_raw.last_updated_stamp is not null,
        unix_timestamp(to_utc_timestamp(o_raw.last_updated_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS last_updated_stamp,
    hour as hour
from tmp.tmp_fd_romeo_order_inv_reserved_detail
LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'order_inv_reserved_detail_id', 'status', 'order_id', 'order_item_id', 'goods_number', 'product_id', 'order_inv_reserved_id', 'reserved_quantity', 'reserved_time', 'status_id', 'facility_id', 'version', 'created_stamp', 'last_updated_stamp') o_raw
AS `table`, ts, `commit`, xid, type, old, order_inv_reserved_detail_id, status, order_id, order_item_id, goods_number, product_id, order_inv_reserved_id, reserved_quantity, reserved_time, status_id, facility_id, version, created_stamp, last_updated_stamp
where dt = '${hiveconf:dt}';
