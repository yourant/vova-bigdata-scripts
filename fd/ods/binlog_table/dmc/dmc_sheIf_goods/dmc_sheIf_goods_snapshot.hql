CREATE TABLE IF NOT EXISTS ods_fd_dmc.ods_fd_dmc_sheIf_goods (
    `id` bigint comment '',
    `goods_sn` string comment '商品编号',
    `extend_goods_id` bigint comment 'editor生成的goods_id(网站id)',
    `status` string comment '0:未上架 1：等待上架：2：已上架',
    `up_sheIf_time` bigint comment '上架时间',
    `supplier_image_url` string comment '供应商图片',
    `modify_image_url` string comment '修图',
    `cat_id` bigint comment '品类id',
    `pg_id` bigint comment '找款providerGoods表主键id',
    `supplier_goods_sn` string comment '供应商品产品编号',
    `purchase_price` string comment '采购价 精确到分',
    `market_price` string comment '市场价',
    `weight` string comment '重量',
    `note` string comment '上架备注',
    `exception` string comment '定时上架异常原因',
    `source_type` string comment 'FT:运营 TS：测试 MZ：贸宗',
    `fill_in` string comment 'Y：填资料 N：未填资料',
    `source_desc` string comment '来源描述',
    `source_link` string comment '来源链接',
    `source_name` string comment '来源名称',
    `risk_level` string comment '风控等级：H_DANGER：一级,M_DANGER：二级,L_DANGER：三级,DANGER：四级,L_SECURE：五级,SECURE：六级',
    `is_tort` string comment '是否侵权 N :未侵权 Y：已侵权',
    `test_location` string comment '测试坑位',
    `is_sort` string comment '是否排序 Y:已排序 N：未排序',
    `supplier_remark` string comment '供应商备注',
    `updated_at` bigint comment '',
    `created_at` bigint comment '',
    `deleted_at` bigint comment '',
    `find_consignor` string comment '选款人',
    `is_modify_image` bigint comment '是否已修图 0：否 1：是',
    `submit_time` bigint comment '贸综提交时间',
    `task_time` bigint comment '找款任务开始时间'
) COMMENT '上架商品对应组织'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_dmc.ods_fd_dmc_sheIf_goods
select `(dt)?+.+` from ods_fd_dmc.ods_fd_dmc_sheIf_goods_arc where dt = '${hiveconf:dt}';
