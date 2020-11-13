CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_region_inc (
    -- maxwell event data
    event_id STRING,
    event_table STRING,
    event_type STRING,
    event_commit BOOLEAN,
    event_date BIGINT,
    -- now data
    region_id int,
	parent_id int,
	region_name string,
	region_type tinyint,
	region_cn_name string,
	region_code string
) COMMENT '来自kafka erp表每日增量数据'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY")
;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_region_inc  PARTITION (dt='${hiveconf:dt}',hour)
select 
    o_raw.xid AS event_id
    ,o_raw.`table` AS event_table
    ,o_raw.type AS event_type
    ,cast(o_raw.`commit` AS BOOLEAN) AS event_commit
    ,cast(o_raw.ts AS BIGINT) AS event_date
    ,o_raw.region_id
	,o_raw.parent_id
	,o_raw.region_name
	,o_raw.region_type
	,o_raw.region_cn_name
	,o_raw.region_code
    ,hour as hour
from tmp.tmp_fd_ecs_region
LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'region_id', 'parent_id', 'region_name', 'region_type', 'region_cn_name', 'region_code') o_raw
AS `table`, ts, `commit`, xid, type, old, region_id, parent_id, region_name, region_type, region_cn_name, region_code
where dt = '${hiveconf:dt}';
