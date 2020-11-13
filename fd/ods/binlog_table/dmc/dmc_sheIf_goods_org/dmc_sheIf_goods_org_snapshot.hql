CREATE TABLE IF NOT EXISTS ods_fd_dmc.ods_fd_dmc_sheIf_goods_org (
    `id` bigint comment '',
    `org_name` string comment '组织名称',
    `virtual_id` bigint comment '虚拟id',
    `sheIf_goods_id` bigint comment '上架商品表主键id',
    `updated_at` bigint comment '',
    `created_at` bigint comment '',
    `deleted_at` bigint comment '',
    `party_id` string comment '',
    `is_sheIf` bigint comment '是否在该组织上过架 0：未上架 1：已上架 2：等待上架',
    `operate_price_user_email` string comment '运营定价人邮箱',
    `operate_price_user` string comment '运营定价人',
    `sheIf_time` bigint comment '上架时间',
    `sheIf_user_email` string comment '上架用户邮箱',
    `sheIf_user` string comment '上架用户名称',
    `sheIf_note` string comment '上架备注',
    `extend_goods_id` bigint comment 'editor生成的goodsId',
    `pg_id` bigint comment 'fam_provider_goods表主键id',
    `status` string comment 'fam_provider_goods_feedback表pgf_status状态',
    `test_location` string comment '测试坑位',
    `test_note` string comment '测试备注'
) COMMENT '上架商品对应组织'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_dmc.ods_fd_dmc_sheIf_goods_org
select `(dt)?+.+` from ods_fd_dmc.ods_fd_dmc_sheIf_goods_org_arc where dt = '${hiveconf:dt}';
