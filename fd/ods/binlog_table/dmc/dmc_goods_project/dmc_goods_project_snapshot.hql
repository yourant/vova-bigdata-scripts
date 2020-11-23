CREATE TABLE IF NOT EXISTS ods_fd_dmc.ods_fd_dmc_goods_project (
    `id` bigint comment 'id',
    `goods_id` bigint,
    `party_id` bigint comment '组织',
    `created_at` bigint comment '生成时间戳bigint',
    `updated_at` bigint comment '更新时间戳bigint',
    `deleted_at` string,
    `on_sale_time` string comment '上架时间',
    `on_sale_staff` string comment '上架人',
    `off_sale_time` string comment '下架时间',
    `shop_price` decimal(15, 4) comment '商品价格，按项目分',
    `market_price` decimal(15, 4),
    `is_tort` string comment '是否侵权 N :未侵权 Y：已侵权',
    `risk_level` string comment '风控等级：h_danger：一级,m_danger：二级,l_danger：三级,danger：四级,l_secure：五级,secure：六级',
    `is_on_sale` bigint,
    `is_delete` bigint,
    `is_display` bigint,
    `virtual_goods_id` string comment '虚拟id',
    `goods_selector` string comment '选款人'
) COMMENT '商品组织信息表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_dmc.ods_fd_dmc_goods_project
select `(pt)?+.+` from ods_fd_dmc.ods_fd_dmc_goods_project_arc where pt = '${hiveconf:pt}';
