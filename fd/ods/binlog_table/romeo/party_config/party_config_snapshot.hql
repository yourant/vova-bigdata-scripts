CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_party_config (
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
) COMMENT '来自对应arc表的数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_party_config
select `(dt)?+.+` from ods_fd_romeo.ods_fd_romeo_party_config_arc where dt = '${hiveconf:dt}';
