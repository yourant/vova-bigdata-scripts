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
) COMMENT '来自kafka erp currency_conversion数据'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_party_arc PARTITION (dt = '${hiveconf:dt}')
select 
     romeo_party_id, created_stamp, last_update_stamp, created_tx_stamp, last_update_tx_stamp, name, description, status, party_id, parent_party_id, is_leaf, short_name, short_party_name, fc_id
from (

    select 
        dt,romeo_party_id, created_stamp, last_update_stamp, created_tx_stamp, last_update_tx_stamp, name, description, status, party_id, parent_party_id, is_leaf, short_name, short_party_name, fc_id,
        row_number () OVER (PARTITION BY romeo_party_id ORDER BY dt DESC) AS rank
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
                fc_id
        from ods_fd_romeo.ods_fd_romeo_party_arc where dt = '${hiveconf:dt_last}'

        UNION

        select dt,romeo_party_id, created_stamp, last_update_stamp, created_tx_stamp, last_update_tx_stamp, name, description, status, party_id, parent_party_id, is_leaf, short_name, short_party_name, fc_id
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

