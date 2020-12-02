create table if not exists dwb.dwb_fd_120_position_impression_uv_7d(
    `project_name` string,
    `platform_name` string,
    `route_sn` string,
    `route_name` string,
    `country` string,
    `absolute_position` bigint,
    `impression_uv` bigint,
    `report_time` string
) comment '7天分类列表页前120坑位的impression'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    stored as parquet;