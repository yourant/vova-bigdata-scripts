CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_ok_coupon_config
(
    `coupon_config_id` bigint COMMENT '',
    `site_id` bigint COMMENT '启用端编号',
    `party_id` bigint,
    `coupon_config_value` decimal(15, 4) COMMENT '红包的价值',
    `coupon_config_status` bigint COMMENT '红包的使用状态：0 启用；1 禁用',
    `coupon_config_stime` bigint COMMENT '红包启用的开始时间, start time',
    `coupon_config_etime` bigint COMMENT '红包启用的结束时间, end time',
    `coupon_config_comment` string COMMENT '红包类型注释',
    `coupon_config_coupon_type` string COMMENT '是值还是百分比',
    `coupon_config_apply_type` string COMMENT '是针对商品价格还是针对运费',
    `coupon_config_data` string COMMENT '红包配置',
    `coupon_config_type_id` bigint COMMENT '红包的类型（促销，返现，补偿）',
    `coupon_config_minimum_amount` decimal(15, 4) COMMENT '超过此值方可使用该优惠券',
    `cat_id` string COMMENT '跟分类关联',
    `goods_id` string,
    `goods_attr` string COMMENT '商品属性',
    `platform` string COMMENT '跟平台关联(PC,H5,APP)',
    `region_code` string COMMENT '跟国家关联',
    `created_by` bigint COMMENT '创建人',
    `created_datetime` string COMMENT '创建时间',
    `currency_id` bigint COMMENT '币种ID',
    `currency` string COMMENT 'USD HKD',
    `coupon_config_minimum_goods_number` bigint
 )comment '红包'
STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_ok_coupon_config
select `(dt)?+.+` from ods_fd_vb.ods_fd_ok_coupon_config_arc where dt = '${hiveconf:dt}';
