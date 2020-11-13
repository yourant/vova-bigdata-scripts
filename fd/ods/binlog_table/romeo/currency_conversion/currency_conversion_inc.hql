CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_currency_conversion_inc (
    -- maxwell event data
    event_id STRING,
    event_table STRING,
    event_type STRING,
    event_commit BOOLEAN,
    event_date BIGINT,
    -- now data
    currency_conversion_id string,
	created_stamp bigint,
	last_update_stamp bigint,
	last_update_tx_stamp bigint,
	created_tx_stamp bigint,
	currency_conversion_date bigint,
	currency_conversion_rate decimal(19, 4),
	created_user_by_login string,
	from_currency_code string,
	to_currency_code string,
	cancellation_flag char(1)
) COMMENT '来自kafka erp订单每日增量数据'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY")
;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_currency_conversion_inc  PARTITION (dt='${hiveconf:dt}',hour)
select 
    o_raw.xid AS event_id
    ,o_raw.`table` AS event_table
    ,o_raw.type AS event_type
    ,cast(o_raw.`commit` AS BOOLEAN) AS event_commit
    ,cast(o_raw.ts AS BIGINT) AS event_date
    ,o_raw.currency_conversion_id
    ,if(o_raw.created_stamp != '0000-00-00 00:00:00', unix_timestamp(to_utc_timestamp(o_raw.created_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS created_stamp
    ,if(o_raw.last_update_stamp != '0000-00-00 00:00:00', unix_timestamp(to_utc_timestamp(o_raw.last_update_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS last_update_stamp
    ,if(o_raw.last_update_tx_stamp != '0000-00-00 00:00:00', unix_timestamp(to_utc_timestamp(o_raw.last_update_tx_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS last_update_tx_stamp
    ,if(o_raw.created_tx_stamp != '0000-00-00 00:00:00', unix_timestamp(to_utc_timestamp(o_raw.created_tx_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS created_tx_stamp
    ,if(o_raw.currency_conversion_date != '0000-00-00 00:00:00' and o_raw.currency_conversion_date != '', unix_timestamp(to_utc_timestamp(o_raw.currency_conversion_date, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS currency_conversion_date
    ,o_raw.currency_conversion_rate
    ,o_raw.created_user_by_login
    ,o_raw.from_currency_code
    ,o_raw.to_currency_code
    ,o_raw.cancellation_flag
    ,hour as hour
from tmp.tmp_fd_romeo_currency_conversion
LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'currency_conversion_id', 'created_stamp', 'last_update_stamp', 'last_update_tx_stamp', 'created_tx_stamp', 'currency_conversion_date', 'currency_conversion_rate', 'created_user_by_login', 'from_currency_code', 'to_currency_code', 'cancellation_fla') o_raw
AS `table`, ts, `commit`, xid, type, old, currency_conversion_id, created_stamp, last_update_stamp, last_update_tx_stamp, created_tx_stamp, currency_conversion_date, currency_conversion_rate, created_user_by_login, from_currency_code, to_currency_code, cancellation_flag
where dt = '${hiveconf:dt}';
