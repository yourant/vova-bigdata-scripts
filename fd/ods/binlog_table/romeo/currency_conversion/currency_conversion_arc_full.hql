CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_currency_conversion_arc (
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
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_currency_conversion_arc PARTITION (dt = '${hiveconf:dt}')
select 
     currency_conversion_id, created_stamp, last_update_stamp, last_update_tx_stamp, created_tx_stamp, currency_conversion_date, currency_conversion_rate, created_user_by_login, from_currency_code, to_currency_code, cancellation_flag
from (

    select 
        dt,currency_conversion_id, created_stamp, last_update_stamp, last_update_tx_stamp, created_tx_stamp, currency_conversion_date, currency_conversion_rate, created_user_by_login, from_currency_code, to_currency_code, cancellation_flag,
        row_number () OVER (PARTITION BY currency_conversion_id ORDER BY dt DESC) AS rank
    from (

        select  '2020-01-01' as dt
                ,currency_conversion_id
                ,if(created_stamp != '0000-00-00 00:00:00', unix_timestamp(to_utc_timestamp(created_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS created_stamp
                ,if(last_update_stamp != '0000-00-00 00:00:00', unix_timestamp(to_utc_timestamp(last_update_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS last_update_stamp
                ,if(last_update_tx_stamp != '0000-00-00 00:00:00', unix_timestamp(to_utc_timestamp(last_update_tx_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS last_update_tx_stamp
                ,if(created_tx_stamp != '0000-00-00 00:00:00', unix_timestamp(to_utc_timestamp(created_tx_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS created_tx_stamp
                ,if(currency_conversion_date != '0000-00-00 00:00:00' and currency_conversion_date != '', unix_timestamp(to_utc_timestamp(currency_conversion_date, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS currency_conversion_date
                ,currency_conversion_rate
                ,created_user_by_login
                ,from_currency_code
                ,to_currency_code
                ,cancellation_flag
        from tmp.tmp_fd_romeo_currency_conversion_full

        UNION

        select dt,currency_conversion_id, created_stamp, last_update_stamp, last_update_tx_stamp, created_tx_stamp, currency_conversion_date, currency_conversion_rate, created_user_by_login, from_currency_code, to_currency_code, cancellation_flag
        from (

            select  dt
                    currency_conversion_id,
                    created_stamp,
                    last_update_stamp,
                    last_update_tx_stamp,
                    created_tx_stamp,
                    currency_conversion_date,
                    currency_conversion_rate,
                    created_user_by_login,
                    from_currency_code,
                    to_currency_code,
                    cancellation_flag
                    row_number () OVER (PARTITION BY currency_conversion_id ORDER BY event_id DESC) AS rank
            from ods_fd_romeo.ods_fd_romeo_currency_conversion_inc where dt='${hiveconf:dt}'

        ) inc where inc.rank = 1
    ) arc 
) tab where tab.rank = 1;
