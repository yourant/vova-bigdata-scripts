CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_currency_conversion (
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
) COMMENT '来自对应arc表的数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_currency_conversion
select `(dt)?+.+` from ods_fd_romeo.ods_fd_romeo_currency_conversion_arc where dt = '${hiveconf:dt}';
