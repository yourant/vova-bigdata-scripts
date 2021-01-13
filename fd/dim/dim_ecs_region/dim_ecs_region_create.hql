CREATE TABLE IF NOT EXISTS dim.dim_fd_ecs_region
(
    region_id bigint,
	parent_id bigint,
	region_name string,
	region_type bigint,
	region_cn_name string,
	region_code string
) comment 'erp region dim表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUET;