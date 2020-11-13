CREATE TABLE IF NOT EXISTS ods_fd_dmc.ods_fd_dmc_party (
    `party_id` string comment 'party_id',
    `created_at` bigint comment '',
    `updated_at` bigint comment '',
    `name` string comment '正常组织全名',
    `lower_name` string comment '小写组织全名',
    `short_party_name` string comment '组织缩写',
    `platform` string comment '组织所属平台：fam, shopify等'
) COMMENT '组织表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_dmc.ods_fd_dmc_party
select `(dt)?+.+` from ods_fd_dmc.ods_fd_dmc_party_arc where dt = '${hiveconf:dt}';
