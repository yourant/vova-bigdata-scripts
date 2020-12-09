CREATE TABLE IF NOT EXISTS ods_fd_erp.dmc_competing_website_info_arc (
    `site_id` bigint comment '',
    `created_at` bigint comment '',
    `updated_at` bigint comment '',
    `site_name` string comment '网站名称',
    `note` string comment '备注'
) COMMENT 'erp 增量同步dmc_competing_website_info'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_erp.dmc_competing_website_info_arc PARTITION (pt = '${hiveconf:pt}')
select 
     site_id, created_at, updated_at, site_name, note
from (

    select 
        dt,site_id, created_at, updated_at, site_name, note,
        row_number () OVER (PARTITION BY site_id ORDER BY dt DESC) AS rank
    from (

        select  '2020-01-01' as dt,
                site_id,
                if(created_at != '0000-00-00 00:00:00', unix_timestamp(created_at, "yyyy-MM-dd HH:mm:ss"), 0) AS created_at,
                if(updated_at != '0000-00-00 00:00:00', unix_timestamp(updated_at, "yyyy-MM-dd HH:mm:ss"), 0) AS updated_at,
                site_name,
                note
        from tmp.tmp_fd_dmc_competing_website_info_full

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
