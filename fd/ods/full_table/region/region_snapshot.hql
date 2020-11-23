CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_region
(
    `region_id` bigint COMMENT '',
    `parent_id` string COMMENT '',
    `region_name` string COMMENT '',
    `region_type` bigint COMMENT '',
    `region_display` bigint COMMENT '',
    `region_code` string COMMENT '',
    `area_id` bigint COMMENT '区域ID',
    `display_order` string COMMENT '国家排序',
    `last_update_time` string COMMENT '最后更新时间',
    `time_zone` bigint COMMENT '',
    `chinese_region_name` bigint COMMENT '',
    `prefix` string COMMENT '国家的手机号前缀'
 )comment '区域'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_region
select `(pt)?+.+` from ods_fd_vb.ods_fd_region_arc where pt = '${hiveconf:pt}';
