CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_party_config_inc (
    -- maxwell event data
    `event_id` STRING,
    `event_table` STRING,
    `event_type` STRING,
    `event_commit` BOOLEAN,
    `event_date` BIGINT,
    -- now data
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
) COMMENT '来自kafka erp表每日增量数据'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_party_config_inc  PARTITION (dt='${hiveconf:dt}',hour)
select 
    o_raw.xid AS event_id
    ,o_raw.`table` AS event_table
    ,o_raw.type AS event_type
    ,cast(o_raw.`commit` AS BOOLEAN) AS event_commit
    ,cast(o_raw.ts AS BIGINT) AS event_date
    ,o_raw.party_id
    ,o_raw.is_auto_confirm
    ,o_raw.is_auto_create_dispatch
    ,o_raw.default_deliver_date
    ,o_raw.average_period_of_production
    ,o_raw.is_calculate_period_production
    ,o_raw.action_user
    ,if(o_raw.last_update_time != '0000-00-00 00:00:00', unix_timestamp(to_utc_timestamp(o_raw.last_update_time, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS last_update_time
    ,o_raw.cms_address
    ,o_raw.ticket_address
    ,o_raw.shopping_address
    ,o_raw.shipping_fee_address
    ,o_raw.customize_fee_address
    ,o_raw.party_code
    ,o_raw.from_domain
    ,o_raw.start_id
    ,o_raw.end_id
    ,o_raw.redundance
    ,o_raw.electronic_bill
    ,o_raw.`name`
    ,o_raw.logo
    ,o_raw.size_type
    ,o_raw.domain_group
    ,hour as hour
from tmp.tmp_fd_romeo_party_config
LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'party_id', 'is_auto_confirm', 'is_auto_create_dispatch', 'default_deliver_date', 'average_period_of_production', 'is_calculate_period_production', 'action_user', 'last_update_time', 'cms_address', 'ticket_address', 'shopping_address', 'shipping_fee_address', 'customize_fee_address', 'party_code', 'from_domain', 'start_id', 'end_id', 'redundance', 'electronic_bill', 'name', 'logo', 'size_type', 'domain_group') o_raw
AS `table`, ts, `commit`, xid, type, old, party_id, is_auto_confirm, is_auto_create_dispatch, default_deliver_date, average_period_of_production, is_calculate_period_production, action_user, last_update_time, cms_address, ticket_address, shopping_address, shipping_fee_address, customize_fee_address, party_code, from_domain, start_id, end_id, redundance, electronic_bill, `name`, logo, size_type, domain_group
where dt = '${hiveconf:dt}';
