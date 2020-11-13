CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_fd_stock_ecs_order_sale_bak_detail_inc (
    -- maxwell event data
    `event_id` STRING,
    `event_table` STRING,
    `event_type` STRING,
    `event_commit` BOOLEAN,
    `event_date` BIGINT,
    -- now data
    id bigint comment '自增id',
	bak_id bigint comment '',
	bak_order_date bigint comment '',
	external_goods_id bigint comment '',
	on_sale_time bigint comment '',
	7d_sale decimal(15, 4) comment '',
	14d_sale decimal(15, 4) comment '',
	28d_sale decimal(15, 4) comment '',
	uniq_sku string comment ''
) COMMENT '来自kafka erp表每日增量数据'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_fd_stock_ecs_order_sale_bak_detail_inc  PARTITION (dt='${hiveconf:dt}',hour)
select 
    o_raw.xid AS event_id,
    o_raw.`table` AS event_table,
    o_raw.type AS event_type,
    cast(o_raw.`commit` AS BOOLEAN) AS event_commit,
    cast(o_raw.ts AS BIGINT) AS event_date,
    o_raw.id,
    o_raw.bak_id,
    /* timezone Asia/Shanghai in mysql ecshop database, convert to UTC */
    if(o_raw.bak_order_date != "0000-00-00 00:00:00", unix_timestamp(to_utc_timestamp(o_raw.bak_order_date, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) as bak_order_date,
    external_goods_id,
    /* timezone Asia/Shanghai in mysql ecshop database, convert to UTC */
    if(o_raw.on_sale_time != "0000-00-00 00:00:00", unix_timestamp(to_utc_timestamp(o_raw.on_sale_time, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) as on_sale_time,
    o_raw.7d_sale,
    o_raw.14d_sale,
    o_raw.28d_sale,
    o_raw.uniq_sku,
    hour as hour
from tmp.tmp_fd_ecs_fd_stock_ecs_order_sale_bak_detail
LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'id', 'bak_id', 'bak_order_date', 'external_goods_id', 'on_sale_time', '7d_sale', '14d_sale', '28d_sale', 'uniq_sku') o_raw
AS `table`, ts, `commit`, xid, type, old, id,bak_id,bak_order_date,external_goods_id,on_sale_time,7d_sale,14d_sale,28d_sale,uniq_sku
where dt = '${hiveconf:dt}';
