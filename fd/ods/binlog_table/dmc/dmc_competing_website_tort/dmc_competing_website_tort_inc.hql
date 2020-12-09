CREATE TABLE IF NOT EXISTS ods_fd_dmc.ods_fd_dmc_competing_website_tort_inc (
    -- maxwell event data
    `event_id` STRING,
    `event_table` STRING,
    `event_type` STRING,
    `event_commit` BOOLEAN,
    `event_date` BIGINT,
    -- now data 
    `id` bigint comment '',
    `created_at` bigint comment '',
    `updated_at` bigint comment '',
    `site_id` bigint comment '网站名称',
    `risk_level`  string comment '风控等级：H_DANGER：一级,M_DANGER：二级,L_DANGER：三级,DANGER：四级,L_SECURE：五级,SECURE：六级,H_SECURE：七级',
    `tort_status` string comment '状态：NEW新建，ENABLED启用，DISABLED弃用'
) COMMENT 'erp 增量同步dmc_competing_website_tort'
PARTITIONED BY (pt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_dmc.ods_fd_dmc_competing_website_tort_inc  PARTITION (pt='${hiveconf:pt}',hour)
select 
    o_raw.xid AS event_id,
    o_raw.`table` AS event_table,
    o_raw.type AS event_type,
    cast(o_raw.`commit` AS BOOLEAN) AS event_commit,
    cast(o_raw.ts AS BIGINT) AS event_date,
    o_raw.id,
    if(o_raw.created_at != '0000-00-00 00:00:00', unix_timestamp(o_raw.created_at, "yyyy-MM-dd HH:mm:ss"), 0) AS created_at,
    if(o_raw.updated_at != '0000-00-00 00:00:00', unix_timestamp(o_raw.updated_at, "yyyy-MM-dd HH:mm:ss"), 0) AS updated_at,
    o_raw.site_id,
    o_raw.risk_level,
    o_raw.tort_status,
    hour as hour
from tmp.tmp_fd_dmc_competing_website_tort
LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'id', 'created_at', 'updated_at', 'site_id', 'risk_level', 'tort_status') o_raw
AS `table`, ts, `commit`, xid, type, old, id, created_at, updated_at, site_id, risk_level, tort_status
where pt = '${hiveconf:pt}';
