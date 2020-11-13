CREATE TABLE IF NOT EXISTS ods_fd_erp.dmc_competing_website_info (
    `site_id` bigint comment '',
    `created_at` bigint comment '',
    `updated_at` bigint comment '',
    `site_name` string comment '网站名称',
    `note` string comment '备注'
) COMMENT 'erp 增量同步dmc_competing_website_info'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_erp.dmc_competing_website_info
select `(dt)?+.+` from ods_fd_erp.dmc_competing_website_info_arc where dt = '${hiveconf:dt}';
