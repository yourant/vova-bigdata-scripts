CREATE TABLE IF NOT EXISTS ods_fd_dmc.ods_fd_dmc_party_inc (
    -- maxwell event data
    `event_id` STRING,
    `event_table` STRING,
    `event_type` STRING,
    `event_commit` BOOLEAN,
    `event_date` BIGINT,
    -- now data 
    `party_id` string comment 'party_id',
    `created_at` bigint comment '',
    `updated_at` bigint comment '',
    `name` string comment '正常组织全名',
    `lower_name` string comment '小写组织全名',
    `short_party_name` string comment '组织缩写',
    `platform` string comment '组织所属平台：fam, shopify等'
) COMMENT '组织表'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_dmc.ods_fd_dmc_party_inc  PARTITION (dt='${hiveconf:dt}',hour)
select 
    o_raw.xid AS event_id,
    o_raw.`table` AS event_table,
    o_raw.type AS event_type,
    cast(o_raw.`commit` AS BOOLEAN) AS event_commit,
    cast(o_raw.ts AS BIGINT) AS event_date,
    o_raw.party_id,
    if(o_raw.created_at != '0000-00-00 00:00:00', unix_timestamp(o_raw.created_at, "yyyy-MM-dd HH:mm:ss"), 0) AS created_at,
    if(o_raw.updated_at != '0000-00-00 00:00:00', unix_timestamp(o_raw.updated_at, "yyyy-MM-dd HH:mm:ss"), 0) AS updated_at,
    o_raw.name,
    o_raw.lower_name,
    o_raw.short_party_name,
    o_raw.platform,
    hour as hour
from tmp.tmp_fd_dmc_party
LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'party_id', 'created_at', 'updated_at', 'name', 'lower_name', 'short_party_name', 'platform') o_raw
AS `table`, ts, `commit`, xid, type, old, party_id, created_at, updated_at, name, lower_name, short_party_name, platform
where dt = '${hiveconf:dt}';
