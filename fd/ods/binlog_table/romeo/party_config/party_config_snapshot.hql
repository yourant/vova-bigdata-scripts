CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_party_config (
    `party_id` bigint COMMENT '组织id',
    `is_auto_confirm` string COMMENT '是否自动确认订单',
    `is_auto_create_dispatch` string COMMENT '是否运行自动生成工单脚本',
    `default_deliver_date` bigint COMMENT '设置默认婚期',
    `average_period_of_production` bigint COMMENT '工单平均制作周期key,0是jjs,1是vb',
    `is_calculate_period_production` string COMMENT '是否按pk_cat_id计算制作周期',
    `action_user` string COMMENT '操作人',
    `last_update_time` bigint COMMENT '操作时间戳bigint',
    `cms_address` string COMMENT 'cms地址',
    `ticket_address` string COMMENT 'ticket地址',
    `shopping_address` string COMMENT '商品地址',
    `shipping_fee_address` string COMMENT '增加运费地址',
    `customize_fee_address` string COMMENT '增加定制费地址',
    `party_code` string COMMENT '区分组织 0：其他组织 1：jjs组织 2:vb组织',
    `from_domain` string COMMENT 'from_domain',
    `start_id` bigint,
    `end_id` bigint,
    `redundance` bigint,
    `electronic_bill` string COMMENT '电子账单',
    `name` string COMMENT 'name',
    `logo` string COMMENT 'logo',
    `size_type` string COMMENT '尺码表类型',
    `domain_group` string COMMENT '分类'
) COMMENT '来自对应arc表的数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_party_config
select `(pt)?+.+` from ods_fd_romeo.ods_fd_romeo_party_config_arc where pt = '${hiveconf:pt}';
