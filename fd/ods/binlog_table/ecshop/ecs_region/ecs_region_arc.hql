CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_region_arc (
    region_id bigint,
	parent_id bigint,
	region_name string,
	region_type bigint,
	region_cn_name string,
	region_code string
) COMMENT '来自kafka erp currency_conversion数据'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_region_arc PARTITION (pt = '${hiveconf:dt}')
select 
     region_id, parent_id, region_name, region_type, region_cn_name, region_code
from (

    select 
        dt,region_id, parent_id, region_name, region_type, region_cn_name, region_code,
        row_number () OVER (PARTITION BY region_id ORDER BY dt DESC) AS rank
    from (

        select  dt
                region_id,
                parent_id,
                region_name,
                region_type,
                region_cn_name,
                region_code
        from ods_fd_ecshop.ods_fd_ecs_region_arc where dt = '${hiveconf:dt_last}'

        UNION

        select dt,region_id, parent_id, region_name, region_type, region_cn_name, region_code
        from (

            select  dt
                    region_id,
                    parent_id,
                    region_name,
                    region_type,
                    region_cn_name,
                    region_code,
                    row_number () OVER (PARTITION BY region_id ORDER BY event_id DESC) AS rank
            from ods_fd_ecshop.ods_fd_ecs_region_inc where dt='${hiveconf:dt}'

        ) inc where inc.rank = 1
    ) arc 
) tab where tab.rank = 1;
