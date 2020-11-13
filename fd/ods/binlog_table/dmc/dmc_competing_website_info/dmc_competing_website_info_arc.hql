CREATE TABLE IF NOT EXISTS ods_fd_erp.dmc_competing_website_info_arc (
    `site_id` bigint comment '',
    `created_at` bigint comment '',
    `updated_at` bigint comment '',
    `site_name` string comment '网站名称',
    `note` string comment '备注'
) COMMENT 'erp 增量同步dmc_competing_website_info'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_erp.dmc_competing_website_info_arc PARTITION (dt = '${hiveconf:dt}')
select 
     site_id, created_at, updated_at, site_name, note
from (

    select 
        dt,site_id, created_at, updated_at, site_name, note,
        row_number () OVER (PARTITION BY site_id ORDER BY dt DESC) AS rank
    from (

        select  dt,
                site_id,
                created_at,
                updated_at,
                site_name,
                note
        from ods_fd_erp.dmc_competing_website_info_arc  where dt = '${hiveconf:dt_last}' 

        UNION

        select dt,site_id, created_at, updated_at, site_name, note
        from (

            select  dt,
                    site_id,
                    created_at,
                    updated_at,
                    site_name,
                    note,
                    row_number() OVER (PARTITION BY site_id ORDER BY event_id DESC) AS rank
            from ods_fd_erp.dmc_competing_website_info_inc where dt='${hiveconf:dt}'

        ) inc where inc.rank = 1
    ) arc 
) tab where tab.rank = 1;
