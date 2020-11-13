CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_party_inc (
    -- maxwell event data
    `event_id` STRING,
    `event_table` STRING,
    `event_type` STRING,
    `event_commit` BOOLEAN,
    `event_date` BIGINT,
    -- now data
    `romeo_party_id` bigint,
    `created_stamp` bigint,
    `last_update_stamp` bigint,
    `created_tx_stamp` bigint,
    `last_update_tx_stamp` bigint,
    `name` string,
    `description` string,
    `status` string,
    `party_id` bigint,
    `parent_party_id` bigint,
    `is_leaf` string,
    `short_name` string,
    `short_party_name` string COMMENT 'short_name字段用来生成uniq_sku, jjshouse的值为空，故添加此字段，所有组织都需要有short name',
    `fc_id` bigint COMMENT '仓库id'
) COMMENT '来自kafka erp表每日增量数据'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_party_inc  PARTITION (dt='${hiveconf:dt}',hour)
select 
    o_raw.xid AS event_id
    ,o_raw.`table` AS event_table
    ,o_raw.type AS event_type
    ,cast(o_raw.`commit` AS BOOLEAN) AS event_commit
    ,cast(o_raw.ts AS BIGINT) AS event_date
    ,o_raw.romeo_party_id
    ,if(o_raw.created_stamp != '0000-00-00 00:00:00', unix_timestamp(to_utc_timestamp(o_raw.created_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS created_stamp
    ,if(o_raw.last_update_stamp != '0000-00-00 00:00:00', unix_timestamp(to_utc_timestamp(o_raw.last_update_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS last_update_stamp
    ,if(o_raw.created_tx_stamp != '0000-00-00 00:00:00', unix_timestamp(to_utc_timestamp(o_raw.created_tx_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS created_tx_stamp
    ,if(o_raw.last_update_tx_stamp != '0000-00-00 00:00:00', unix_timestamp(to_utc_timestamp(o_raw.last_update_tx_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS last_update_tx_stamp
    ,o_raw.`name`
    ,o_raw.`description`
    ,o_raw.`status`
    ,o_raw.party_id
    ,o_raw.parent_party_id
    ,o_raw.is_leaf
    ,o_raw.short_name
    ,o_raw.short_party_name
    ,o_raw.fc_id
    ,hour as hour
from tmp.tmp_fd_romeo_party
LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'romeo_party_id', 'created_stamp', 'last_update_stamp', 'created_tx_stamp', 'last_update_tx_stamp', 'name', 'description', 'status', 'party_id', 'parent_party_id', 'is_leaf', 'short_name', 'short_party_name', 'fc_id') o_raw
AS `table`, ts, `commit`, xid, type, old, romeo_party_id, created_stamp, last_update_stamp, created_tx_stamp, last_update_tx_stamp, `name`, `description`, `status`, party_id, parent_party_id, is_leaf, short_name, short_party_name, fc_id
where dt = '${hiveconf:dt}';
