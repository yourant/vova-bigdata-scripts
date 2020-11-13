CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_fd_sku_backups (
    `id` bigint COMMENT 'id',
    `uniq_sku` string COMMENT '商品sku',
    `sale_region` bigint COMMENT '是否针对波兰仓的销量，2所有销量，1针对波兰仓',
    `color` string COMMENT 'sku颜色',
    `size` string COMMENT 'sku尺码'
) COMMENT 'fd相关组织所有有销量或者有库存的sku备份'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_fd_sku_backups
select `(dt)?+.+` from ods_fd_ecshop.ods_fd_ecs_fd_sku_backups_arc where dt = '${hiveconf:dt}';
