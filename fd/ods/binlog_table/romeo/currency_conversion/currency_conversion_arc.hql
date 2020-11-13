CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_currency_conversion_arc (
    currency_conversion_id string,
	created_stamp bigint,
	last_update_stamp bigint,
	last_update_tx_stamp bigint,
	created_tx_stamp bigint,
	currency_conversion_date bigint,
	currency_conversion_rate decimal(15, 4),
	created_user_by_login string,
	from_currency_code string,
	to_currency_code string,
	cancellation_flag string
) COMMENT '来自kafka erp currency_conversion数据'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_currency_conversion_arc PARTITION (dt = '${hiveconf:dt}')
select 
     currency_conversion_id, created_stamp, last_update_stamp, last_update_tx_stamp, created_tx_stamp, currency_conversion_date, currency_conversion_rate, created_user_by_login, from_currency_code, to_currency_code, cancellation_flag
from (

    select 
        dt,currency_conversion_id, created_stamp, last_update_stamp, last_update_tx_stamp, created_tx_stamp, currency_conversion_date, currency_conversion_rate, created_user_by_login, from_currency_code, to_currency_code, cancellation_flag,
        row_number () OVER (PARTITION BY currency_conversion_id ORDER BY dt DESC) AS rank
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
        from ods_fd_romeo.ods_fd_romeo_currency_conversion_arc where dt='${hiveconf:dt_last}'
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
