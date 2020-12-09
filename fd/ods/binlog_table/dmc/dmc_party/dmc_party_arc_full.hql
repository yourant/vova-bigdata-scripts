CREATE TABLE IF NOT EXISTS ods_fd_dmc.ods_fd_dmc_party_arc (
    `party_id` string comment 'party_id',
    `created_at` bigint comment '',
    `updated_at` bigint comment '',
    `name` string comment '正常组织全名',
    `lower_name` string comment '小写组织全名',
    `short_party_name` string comment '组织缩写',
    `platform` string comment '组织所属平台：fam, shopify等'
) COMMENT '组织表'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_dmc.ods_fd_dmc_party_arc PARTITION (pt = '${hiveconf:pt}')
select 
     party_id, created_at, updated_at, name, lower_name, short_party_name, platform
from (

    select 
        pt,party_id, created_at, updated_at, name, lower_name, short_party_name, platform,
        row_number () OVER (PARTITION BY party_id ORDER BY pt DESC) AS rank
    from (

        select  '2020-01-01' as pt
                ,party_id
                ,if(created_at != '0000-00-00 00:00:00', unix_timestamp(created_at, "yyyy-MM-dd HH:mm:ss"), 0) AS created_at
                ,if(updated_at != '0000-00-00 00:00:00', unix_timestamp(updated_at, "yyyy-MM-dd HH:mm:ss"), 0) AS updated_at
                ,name
                ,lower_name
                ,short_party_name
                ,platform
        from tmp.tmp_fd_dmc_party_full

        UNION

        select pt,party_id, created_at, updated_at, name, lower_name, short_party_name, platform
        from (

            select  pt,
                    party_id,
                    created_at,
                    updated_at,
                    name,
                    lower_name,
                    short_party_name,
                    platform,
                    row_number() OVER (PARTITION BY party_id ORDER BY event_id DESC) AS rank
            from ods_fd_dmc.ods_fd_dmc_party_inc where pt='${hiveconf:pt}'

        ) inc where inc.rank = 1
    ) arc 
) tab where tab.rank = 1;
