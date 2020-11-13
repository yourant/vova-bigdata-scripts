CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_fd_sku_backups_inc (
    -- maxwell event data
    `event_id` STRING,
    `event_table` STRING,
    `event_type` STRING,
    `event_commit` BOOLEAN,
    `event_date` BIGINT,
    -- now data 
    `id` bigint COMMENT 'id',
    `uniq_sku` string COMMENT '商品sku',
    `sale_region` bigint COMMENT '是否针对波兰仓的销量，2所有销量，1针对波兰仓',
    `color` string COMMENT 'sku颜色',
    `size` string COMMENT 'sku尺码'
) COMMENT 'fd相关组织所有有销量或者有库存的sku备份'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_fd_sku_backups_inc  PARTITION (dt='${hiveconf:dt}',hour)
select 
    o_raw.xid AS event_id,
    o_raw.`table` AS event_table,
    o_raw.type AS event_type,
    cast(o_raw.`commit` AS BOOLEAN) AS event_commit,
    cast(o_raw.ts AS BIGINT) AS event_date,
    o_raw.id,
    o_raw.uniq_sku,
    o_raw.sale_region,
    o_raw.color,
    o_raw.size,
    hour as hour
from tmp.tmp_fd_ecs_fd_sku_backups
LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'id', 'uniq_sku', 'sale_region', 'color', 'size') o_raw
AS `table`, ts, `commit`, xid, type, old, id,uniq_sku,sale_region,color,size
where dt = '${hiveconf:dt}';
