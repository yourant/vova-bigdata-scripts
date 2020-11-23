CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_fd_sku_backups_arc (
    `id` bigint COMMENT 'id',
    `uniq_sku` string COMMENT '商品sku',
    `sale_region` bigint COMMENT '是否针对波兰仓的销量，2所有销量，1针对波兰仓',
    `color` string COMMENT 'sku颜色',
    `size` string COMMENT 'sku尺码'
) COMMENT 'fd相关组织所有有销量或者有库存的sku备份'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_fd_sku_backups_arc PARTITION (pt = '${hiveconf:pt}')
select 
     id,uniq_sku,sale_region,color,size
from (

    select 
        pt,id,uniq_sku,sale_region,color,size,
        row_number () OVER (PARTITION BY id ORDER BY pt DESC) AS rank
    from (

        select  '2020-01-01' as pt,
                id,
                uniq_sku,
                sale_region,
                color,
                size
        from tmp.tmp_fd_ecs_fd_sku_backups_full

        UNION

        select pt,id,uniq_sku,sale_region,color,size
        from (

            select  pt
                    id,
                    uniq_sku,
                    sale_region,
                    color,
                    size,   
                    row_number() OVER (PARTITION BY id ORDER BY event_id DESC) AS rank
            from ods_fd_ecshop.ods_fd_ecs_fd_sku_backups_inc where pt='${hiveconf:pt}'

        ) inc where inc.rank = 1
    ) arc 
) tab where tab.rank = 1;
