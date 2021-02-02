create table if not exists ads.ads_fd_120_position_impression_uv_7d_avg(
    `project_name` string,
    `platform_name` string,
    `route_sn` string,
    `route_name` string,
    `country` string,
    `absolute_position` bigint,
    `impression_uv_avg` DECIMAL(15, 4),
    `data_time` string
) comment '前7天分类列表页前120坑位的impression平均值'
partitioned by (`pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
stored as parquet;

