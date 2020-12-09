CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_region_inc (
    `region_id` string COMMENT '',
    `parent_id` string COMMENT '',
    `region_name` string COMMENT '',
    `region_type` bigint COMMENT '',
    `region_cn_name` string COMMENT '',
    `region_code` string COMMENT '缩写，两个字母or字符'
) COMMENT '来自kafka erp表每日增量数据'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_region_arc (
    `region_id` string COMMENT '',
    `parent_id` string COMMENT '',
    `region_name` string COMMENT '',
    `region_type` bigint COMMENT '',
    `region_cn_name` string COMMENT '',
    `region_code` string COMMENT '缩写，两个字母or字符'
) COMMENT '来自kafka erp currency_conversion数据'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;


CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_region (
    `region_id` string COMMENT '',
    `parent_id` string COMMENT '',
    `region_name` string COMMENT '',
    `region_type` bigint COMMENT '',
    `region_cn_name` string COMMENT '',
    `region_code` string COMMENT '缩写，两个字母or字符'
) COMMENT '来自对应arc表的数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

