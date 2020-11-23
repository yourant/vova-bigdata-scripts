CREATE TABLE IF NOT EXISTS ods_fd_dmc.ods_fd_dmc_competing_website_tort_arc (
    `id` bigint comment '',
    `created_at` bigint comment '',
    `updated_at` bigint comment '',
    `site_id` bigint comment '网站名称',
    `risk_level`  string comment '风控等级：H_DANGER：一级,M_DANGER：二级,L_DANGER：三级,DANGER：四级,L_SECURE：五级,SECURE：六级,H_SECURE：七级',
    `tort_status` string comment '状态：NEW新建，ENABLED启用，DISABLED弃用'
) COMMENT 'erp 增量同步dmc_competing_website_tort'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_dmc.ods_fd_dmc_competing_website_tort_arc PARTITION (pt = '${hiveconf:pt}')
select 
     id, created_at, updated_at, site_id, risk_level, tort_status
from (

    select 
        pt,id, created_at, updated_at, site_id, risk_level, tort_status,
        row_number () OVER (PARTITION BY id ORDER BY pt DESC) AS rank
    from (

        select  pt,
                id,
                created_at,
                updated_at,
                site_id,
                risk_level,
                tort_status
        from ods_fd_dmc.ods_fd_dmc_competing_website_tort_arc  where pt = '${hiveconf:pt_last}'

        UNION

        select pt,id, created_at, updated_at, site_id, risk_level, tort_status
        from (

            select  pt,
                    id,
                    created_at,
                    updated_at,
                    site_id,
                    risk_level,
                    tort_status,
                    row_number() OVER (PARTITION BY id ORDER BY event_id DESC) AS rank
            from ods_fd_dmc.ods_fd_dmc_competing_website_tort_inc where pt='${hiveconf:pt}'

        ) inc where inc.rank = 1
    ) arc 
) tab where tab.rank = 1;
