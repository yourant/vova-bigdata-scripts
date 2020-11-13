CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_party_config_arc (
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
) COMMENT '来自kafka erp订单每日增量数据'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_party_config_arc PARTITION (dt = '${hiveconf:dt}')
select 
     party_id, is_auto_confirm, is_auto_create_dispatch, default_deliver_date, average_period_of_production, is_calculate_period_production, action_user, last_update_time, cms_address, ticket_address, shopping_address, shipping_fee_address, customize_fee_address, party_code, from_domain, start_id, end_id, redundance, electronic_bill, name, logo, size_type, domain_group
from (

    select 
        dt,party_id, is_auto_confirm, is_auto_create_dispatch, default_deliver_date, average_period_of_production, is_calculate_period_production, action_user, last_update_time, cms_address, ticket_address, shopping_address, shipping_fee_address, customize_fee_address, party_code, from_domain, start_id, end_id, redundance, electronic_bill, name, logo, size_type, domain_group,
        row_number () OVER (PARTITION BY party_id ORDER BY dt DESC) AS rank
    from (

        select  '2020-01-01' as dt
                ,party_id
                ,is_auto_confirm
                ,is_auto_create_dispatch
                ,default_deliver_date
                ,average_period_of_production
                ,is_calculate_period_production
                ,action_user
                ,if(last_update_time != '0000-00-00 00:00:00', unix_timestamp(to_utc_timestamp(last_update_time, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS last_update_time
                ,cms_address
                ,ticket_address
                ,shopping_address
                ,shipping_fee_address
                ,customize_fee_address
                ,party_code
                ,from_domain
                ,start_id
                ,end_id
                ,redundance
                ,electronic_bill
                ,name
                ,logo
                ,size_type
                ,domain_group
        from tmp.tmp_fd_romeo_party_config_full

        UNION

        select dt,party_id, is_auto_confirm, is_auto_create_dispatch, default_deliver_date, average_period_of_production, is_calculate_period_production, action_user, last_update_time, cms_address, ticket_address, shopping_address, shipping_fee_address, customize_fee_address, party_code, from_domain, start_id, end_id, redundance, electronic_bill, name, logo, size_type, domain_group
        from (

            select  dt
                    party_id,
                    is_auto_confirm,
                    is_auto_create_dispatch,
                    default_deliver_date,
                    average_period_of_production,
                    is_calculate_period_production,
                    action_user,
                    last_update_time,
                    cms_address,
                    ticket_address,
                    shopping_address,
                    shipping_fee_address,
                    customize_fee_address,
                    party_code,
                    from_domain,
                    start_id,
                    end_id,
                    redundance,
                    electronic_bill,
                    name,
                    logo,
                    size_type,
                    domain_group,
                    row_number () OVER (PARTITION BY party_id ORDER BY event_id DESC) AS rank
            from ods_fd_romeo.ods_fd_romeo_party_config_inc where dt='${hiveconf:dt}'

        ) inc where inc.rank = 1
    ) arc 
) tab where tab.rank = 1;
