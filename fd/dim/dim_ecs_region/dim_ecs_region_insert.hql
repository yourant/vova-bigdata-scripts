INSERT OVERWRITE TABLE dim.dim_fd_ecs_region
SELECT
    /*+ REPARTITION(1) */
    region_id,
	parent_id,
	region_name,
	region_type,
	region_cn_name,
	region_code
FROM ods_fd_ecshop.ods_fd_ecs_region
;