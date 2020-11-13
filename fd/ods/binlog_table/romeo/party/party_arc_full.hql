CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_party_arc (
    `romeo_party_id` bigint,
    `created_stamp` bigint,
    `last_update_stamp` bigint,
    `created_tx_stamp` bigint,
    `last_update_tx_stamp` bigint,
    `name` string,
    `description` string,
    `status` string,
    `party_id` bigint,
    `parent_party_id` bigint,
    `is_leaf` char(1),
    `short_name` string,
    `short_party_name` string COMMENT 'short_name字段用来生成uniq_sku, jjshouse的值为空，故添加此字段，所有组织都需要有short name',
    `fc_id` bigint COMMENT '仓库id'
) COMMENT '来自kafka erp订单每日增量数据'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY")
;


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_party_arc PARTITION (dt = '${hiveconf:dt}')
select 
     romeo_party_id, created_stamp, last_update_stamp, created_tx_stamp, last_update_tx_stamp, name, description, status, party_id, parent_party_id, is_leaf, short_name, short_party_name, fc_id
from (

    select 
        dt,romeo_party_id, created_stamp, last_update_stamp, created_tx_stamp, last_update_tx_stamp, name, description, status, party_id, parent_party_id, is_leaf, short_name, short_party_name, fc_id,
        row_number () OVER (PARTITION BY romeo_party_id ORDER BY dt DESC) AS rank
    from (

        select  '2020-01-01' as dt
                ,romeo_party_id
                ,if(created_stamp != '0000-00-00 00:00:00', unix_timestamp(to_utc_timestamp(created_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS created_stamp
                ,if(last_update_stamp != '0000-00-00 00:00:00', unix_timestamp(to_utc_timestamp(last_update_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS last_update_stamp
                ,if(created_tx_stamp != '0000-00-00 00:00:00', unix_timestamp(to_utc_timestamp(created_tx_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS created_tx_stamp
                ,if(last_update_tx_stamp != '0000-00-00 00:00:00', unix_timestamp(to_utc_timestamp(last_update_tx_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS last_update_tx_stamp
                ,name
                ,description
                ,status
                ,party_id
                ,parent_party_id
                ,is_leaf
                ,short_name
                ,short_party_name
                ,fc_id
        from tmp.tmp_fd_romeo_party_full

        UNION

        select dt,currency_conversion_id, created_stamp, last_update_stamp, last_update_tx_stamp, created_tx_stamp, currency_conversion_date, currency_conversion_rate, created_user_by_login, from_currency_code, to_currency_code, cancellation_flag
        from (

            select  dt
                    romeo_party_id,
                    created_stamp,
                    last_update_stamp,
                    created_tx_stamp,
                    last_update_tx_stamp,
                    name,
                    description,
                    status,
                    party_id,
                    parent_party_id,
                    is_leaf,
                    short_name,
                    short_party_name,
                    fc_id,
                    row_number () OVER (PARTITION BY romeo_party_id ORDER BY event_id DESC) AS rank
            from ods_fd_romeo.ods_fd_romeo_party_inc where dt='${hiveconf:dt}'

        ) inc where inc.rank = 1
    ) arc 
) tab where tab.rank = 1;
