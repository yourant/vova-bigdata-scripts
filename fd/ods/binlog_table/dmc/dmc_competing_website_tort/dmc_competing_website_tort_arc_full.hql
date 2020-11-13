CREATE TABLE IF NOT EXISTS ods_fd_dmc.ods_fd_dmc_competing_website_tort_arc (
    `id` bigint comment '',
    `created_at` bigint comment '',
    `updated_at` bigint comment '',
    `site_id` bigint comment '网站名称',
    `risk_level`  string comment '风控等级：H_DANGER：一级,M_DANGER：二级,L_DANGER：三级,DANGER：四级,L_SECURE：五级,SECURE：六级,H_SECURE：七级',
    `tort_status` string comment '状态：NEW新建，ENABLED启用，DISABLED弃用'
) COMMENT 'erp 增量同步dmc_competing_website_tort'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_dmc.ods_fd_dmc_competing_website_tort_arc PARTITION (dt = '${hiveconf:dt}')
select 
     id, created_at, updated_at, site_id, risk_level, tort_status
from (

    select 
        dt,id, created_at, updated_at, site_id, risk_level, tort_status,
        row_number () OVER (PARTITION BY id ORDER BY dt DESC) AS rank
    from (

        select  '2020-01-01' as dt,
                id,
                if(created_at != '0000-00-00 00:00:00', unix_timestamp(created_at, "yyyy-MM-dd HH:mm:ss"), 0) AS created_at,
                if(updated_at != '0000-00-00 00:00:00', unix_timestamp(updated_at, "yyyy-MM-dd HH:mm:ss"), 0) AS updated_at,
                site_id,
                risk_level,
                tort_status
        from tmp.tmp_fd_dmc_competing_website_tort_full

        UNION

        select dt,id, created_at, updated_at, site_id, risk_level, tort_status
        from (

            select  dt,
                    id,
                    created_at,
                    updated_at,
                    site_id,
                    risk_level,
                    tort_status,
                    row_number() OVER (PARTITION BY id ORDER BY event_id DESC) AS rank
            from ods_fd_dmc.ods_fd_dmc_competing_website_tort_inc where dt='${hiveconf:dt}'

        ) inc where inc.rank = 1
    ) arc 
) tab where tab.rank = 1;
