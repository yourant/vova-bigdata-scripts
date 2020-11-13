CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_fd_sku_backups_arc (
    `id` bigint COMMENT 'id',
    `uniq_sku` string COMMENT '商品sku',
    `sale_region` bigint COMMENT '是否针对波兰仓的销量，2所有销量，1针对波兰仓',
    `color` string COMMENT 'sku颜色',
    `size` string COMMENT 'sku尺码'
) COMMENT 'fd相关组织所有有销量或者有库存的sku备份'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_fd_sku_backups_arc PARTITION (dt = '${hiveconf:dt}')
select 
     id,uniq_sku,sale_region,color,size
from (

    select 
        dt,id,uniq_sku,sale_region,color,size,
        row_number () OVER (PARTITION BY id ORDER BY dt DESC) AS rank
    from (

        select  dt
                id,
                uniq_sku,
                sale_region,
                color,
                size
        from ods_fd_ecshop.ods_fd_ecs_fd_sku_backups_arc where dt = '${hiveconf:dt_last}'

        UNION

        select dt,id,uniq_sku,sale_region,color,size
        from (

            select  dt
                    id,
                    uniq_sku,
                    sale_region,
                    color,
                    size,   
                    row_number() OVER (PARTITION BY id ORDER BY event_id DESC) AS rank
            from ods_fd_ecshop.ods_fd_ecs_fd_sku_backups_inc where dt='${hiveconf:dt}'

        ) inc where inc.rank = 1
    ) arc 
) tab where tab.rank = 1;
