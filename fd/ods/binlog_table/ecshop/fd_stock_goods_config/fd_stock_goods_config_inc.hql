CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_fd_stock_goods_config_inc (
    -- maxwell event data
    event_id STRING,
    event_table STRING,
    event_type STRING,
    event_commit BOOLEAN,
    event_date BIGINT,
    -- now data
    id bigint comment '自增id',
	goods_id bigint comment '外部商品id',
	min_quantity decimal(15, 4) comment '起订量(件)',
	produce_days decimal(15, 4) comment '生产天数',
	change_provider string comment '更换供应商 N:否 Y:是',
	change_provider_days decimal(15, 4) comment '更换供应商天数',
	change_provider_reason string comment '更换供应商原因',
	is_delete bigint comment '是否删除',
	update_date bigint comment '',
	fabric string comment '面料类型:A类-工厂-梭织100米,B类-工厂-针织70米,C类-工厂-呢料60米,D类-工厂-数码印花30米,F类-贸易-其他',
	provider_type string comment '生产类型:A-blouse-工厂：默认7天,B1-dress（素色）-工厂：默认7天,B2-dress（特殊工艺）-工厂：默认10天,C1-coats（无里布）/sweater-工厂：默认10天,C2-Coats（有里布）-工厂：默认12天,C3-Coats（棉服皮草）-工厂：默认15天,D1-Swimwear/shoes-贸易：默认10天,E-时装-贸易：默认7天,F：手填天数'
) COMMENT '来自kafka erp表每日增量数据'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_fd_stock_goods_config_inc  PARTITION (dt='${hiveconf:dt}',hour)
select 
    o_raw.xid AS event_id,
    o_raw.`table` AS event_table,
    o_raw.type AS event_type,
    cast(o_raw.`commit` AS BOOLEAN) AS event_commit,
    cast(o_raw.ts AS BIGINT) AS event_date,
    o_raw.id,
    o_raw.goods_id,
    o_raw.min_quantity,
    o_raw.produce_days,
    o_raw.change_provider,
    o_raw.change_provider_days,
    o_raw.change_provider_reason,
    o_raw.is_delete,
    /* timezone Asia/Shanghai in mysql ecshop database, convert to UTC */
    if(o_raw.update_date != "0000-00-00 00:00:00", unix_timestamp(to_utc_timestamp(o_raw.update_date, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) as update_date,
    o_raw.fabric,
    o_raw.provider_type,
    o_raw.hour as hour
from tmp.tmp_fd_ecs_fd_stock_goods_config
LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'id', 'goods_id', 'min_quantity', 'produce_days', 'change_provider', 'change_provider_days', 'change_provider_reason', 'is_delete', 'update_date', 'fabric', 'provider_type') o_raw
AS `table`, ts, `commit`, xid, type, old, id,goods_id,min_quantity,produce_days,change_provider,change_provider_days,change_provider_reason,is_delete,update_date,fabric,provider_type
where dt = '${hiveconf:dt}';
