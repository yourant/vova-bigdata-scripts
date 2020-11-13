CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_order_inv_reserverd_inventory_mapping_inc (
    -- maxwell event data
    event_id STRING,
    event_table STRING,
    event_type STRING,
    event_commit BOOLEAN,
    event_date BIGINT,
    -- now data
    id                           bigint,
    order_inv_reserved_detail_id string,
    inventory_item_id            string,
    quantity                     int comment '数量',
    created_stamp                bigint,
    last_updated_stamp           bigint
) COMMENT '来自kafka erp表每日增量数据'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY")
;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_order_inv_reserverd_inventory_mapping_inc  PARTITION (dt='${hiveconf:dt}',hour)
select 
    o_raw.xid AS event_id,
    o_raw.`table` AS event_table,
    o_raw.type AS event_type,
    cast(o_raw.`commit` AS BOOLEAN) AS event_commit,
    cast(o_raw.ts AS BIGINT) AS event_date,
    o_raw.id,
    o_raw.order_inv_reserved_detail_id,
    o_raw.inventory_item_id,
    o_raw.quantity,
    if(o_raw.created_stamp != "0000-00-00 00:00:00" or o_raw.created_stamp is not null,
        unix_timestamp(to_utc_timestamp(o_raw.created_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS created_stamp,
    if(o_raw.last_updated_stamp != "0000-00-00 00:00:00" or o_raw.last_updated_stamp is not null,
        unix_timestamp(to_utc_timestamp(o_raw.last_updated_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS last_updated_stamp,
    hour as hour
from tmp.tmp_fd_romeo_order_inv_reserverd_inventory_mapping
LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'id', 'order_inv_reserved_detail_id', 'inventory_item_id', 'quantity', 'created_stamp', 'last_updated_stamp') o_raw
AS `table`, ts, `commit`, xid, type, old, id, order_inv_reserved_detail_id, inventory_item_id, quantity, created_stamp, last_updated_stamp
where dt = '${hiveconf:dt}';

