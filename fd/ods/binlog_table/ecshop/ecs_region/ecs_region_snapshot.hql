CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_region (
    region_id bigint,
    parent_id bigint,
    region_name string,
    region_type bigint,
    region_cn_name string,
    region_code string
) COMMENT '来自对应arc表的数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_region
select `(dt)?+.+` from ods_fd_ecshop.ods_fd_ecs_region_arc where dt = '${hiveconf:dt}';
