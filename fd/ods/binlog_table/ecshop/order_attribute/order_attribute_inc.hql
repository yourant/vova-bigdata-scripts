CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_order_attribute_inc (
    -- maxwell event data
    event_id STRING,
    event_table STRING,
    event_type STRING,
    event_commit BOOLEAN,
    event_date BIGINT,
    -- now data
    attribute_id bigint COMMENT '自增id',
    order_id bigint COMMENT '订单id',
    attr_name string COMMENT '扩展名',
    attr_value string COMMENT '扩展值'
) COMMENT '来自kafka erp表每日增量数据'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY")
;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_order_attribute_inc  PARTITION (dt='${hiveconf:dt}',hour)
select 
    o_raw.xid AS event_id
    ,o_raw.`table` AS event_table
    ,o_raw.type AS event_type
    ,cast(o_raw.`commit` AS BOOLEAN) AS event_commit
    ,cast(o_raw.ts AS BIGINT) AS event_date
    ,o_raw.attribute_id,
    ,o_raw.order_id,
    ,o_raw.attr_name,
    ,o_raw.attr_value
    ,hour as hour
from tmp.tmp_fd_ecs_order_attribute
LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'attribute_id', 'order_id', 'attr_name', 'attr_value') o_raw
AS `table`, ts, `commit`, xid, type, old, attribute_id, order_id, attr_name, attr_value
where dt = '${hiveconf:dt}';
