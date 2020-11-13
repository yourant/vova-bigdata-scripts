CREATE TABLE IF NOT EXISTS ods_fd_erp.dmc_competing_website_info_inc (
    -- maxwell event data
    `event_id` STRING,
    `event_table` STRING,
    `event_type` STRING,
    `event_commit` BOOLEAN,
    `event_date` BIGINT,
    -- now data 
    `site_id` bigint comment '',
    `created_at` bigint comment '',
    `updated_at` bigint comment '',
    `site_name` string comment '网站名称',
    `note` string comment '备注'
) COMMENT 'erp 增量同步dmc_competing_website_info'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_erp.dmc_competing_website_info_inc  PARTITION (dt='${hiveconf:dt}',hour)
select 
    o_raw.xid AS event_id,
    o_raw.`table` AS event_table,
    o_raw.type AS event_type,
    cast(o_raw.`commit` AS BOOLEAN) AS event_commit,
    cast(o_raw.ts AS BIGINT) AS event_date,
    o_raw.site_id,
    if(o_raw.created_at != '0000-00-00 00:00:00', unix_timestamp(o_raw.created_at, "yyyy-MM-dd HH:mm:ss"), 0) AS created_at,
    if(o_raw.updated_at != '0000-00-00 00:00:00', unix_timestamp(o_raw.updated_at, "yyyy-MM-dd HH:mm:ss"), 0) AS updated_at,
    o_raw.site_name,
    o_raw.note,
    hour as hour
from tmp.tmp_fd_dmc_competing_website_info
LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'site_id', 'created_at', 'updated_at', 'site_name', 'note') o_raw
AS `table`, ts, `commit`, xid, type, old, site_id, created_at, updated_at, site_name, note
where dt = '${hiveconf:dt}';
