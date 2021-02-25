CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_order_goods_inc (
    `rec_id` bigint COMMENT '',
    `order_id` bigint COMMENT '',
    `goods_id` bigint COMMENT '',
    `goods_name` string COMMENT '',
    `goods_sn` string COMMENT '',
    `goods_number` bigint COMMENT '',
    `market_price` decimal(13,4) COMMENT '',
    `goods_price` decimal(13,4) COMMENT '',
    `goods_attr` string COMMENT '',
    `send_number` string COMMENT '',
    `is_real` bigint COMMENT '',
    `extension_code` string COMMENT '',
    `parent_id` string COMMENT '',
    `is_gift` string COMMENT '',
    `goods_status` bigint COMMENT '',
    `action_amt` decimal(13,4) COMMENT '',
    `action_reason_cat` bigint COMMENT '',
    `action_note` string COMMENT '',
    `carrier_bill_id` bigint COMMENT '',
    `provider_id` bigint COMMENT '',
    `invoice_num` string COMMENT '',
    `return_points` bigint COMMENT '',
    `return_bonus` string COMMENT '',
    `biaoju_store_goods_id` bigint COMMENT '',
    `subtitle` string COMMENT '',
    `addtional_shipping_fee` bigint COMMENT '',
    `style_id` string COMMENT '',
    `customized` string COMMENT '',
    `status_id` string COMMENT '商品新旧状态',
    `added_fee` decimal(13,4) COMMENT '税率',
    `external_order_goods_id` bigint COMMENT '网站order_goods_id'
) COMMENT '来自kafka erp表每日增量数据'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_order_goods_binlog_inc (
    `rec_id` bigint COMMENT '',
    `order_id` bigint COMMENT '',
    `goods_id` bigint COMMENT '',
    `goods_name` string COMMENT '',
    `goods_sn` string COMMENT '',
    `goods_number` bigint COMMENT '',
    `market_price` decimal(13,4) COMMENT '',
    `goods_price` decimal(13,4) COMMENT '',
    `goods_attr` string COMMENT '',
    `send_number` string COMMENT '',
    `is_real` bigint COMMENT '',
    `extension_code` string COMMENT '',
    `parent_id` string COMMENT '',
    `is_gift` string COMMENT '',
    `goods_status` bigint COMMENT '',
    `action_amt` decimal(13,4) COMMENT '',
    `action_reason_cat` bigint COMMENT '',
    `action_note` string COMMENT '',
    `carrier_bill_id` bigint COMMENT '',
    `provider_id` bigint COMMENT '',
    `invoice_num` string COMMENT '',
    `return_points` bigint COMMENT '',
    `return_bonus` string COMMENT '',
    `biaoju_store_goods_id` bigint COMMENT '',
    `subtitle` string COMMENT '',
    `addtional_shipping_fee` bigint COMMENT '',
    `style_id` string COMMENT '',
    `customized` string COMMENT '',
    `status_id` string COMMENT '商品新旧状态',
    `added_fee` decimal(13,4) COMMENT '税率',
    `external_order_goods_id` bigint COMMENT '网站order_goods_id',
    `event_type` string COMMENT 'binlog事件类型：update delete ...'
) COMMENT '来自kafka erp表每日增量数据'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_order_goods_arc (
    `rec_id` bigint COMMENT '',
    `order_id` bigint COMMENT '',
    `goods_id` bigint COMMENT '',
    `goods_name` string COMMENT '',
    `goods_sn` string COMMENT '',
    `goods_number` bigint COMMENT '',
    `market_price` decimal(13,4) COMMENT '',
    `goods_price` decimal(13,4) COMMENT '',
    `goods_attr` string COMMENT '',
    `send_number` string COMMENT '',
    `is_real` bigint COMMENT '',
    `extension_code` string COMMENT '',
    `parent_id` string COMMENT '',
    `is_gift` string COMMENT '',
    `goods_status` bigint COMMENT '',
    `action_amt` decimal(13,4) COMMENT '',
    `action_reason_cat` bigint COMMENT '',
    `action_note` string COMMENT '',
    `carrier_bill_id` bigint COMMENT '',
    `provider_id` bigint COMMENT '',
    `invoice_num` string COMMENT '',
    `return_points` bigint COMMENT '',
    `return_bonus` string COMMENT '',
    `biaoju_store_goods_id` bigint COMMENT '',
    `subtitle` string COMMENT '',
    `addtional_shipping_fee` bigint COMMENT '',
    `style_id` string COMMENT '',
    `customized` string COMMENT '',
    `status_id` string COMMENT '商品新旧状态',
    `added_fee` decimal(13,4) COMMENT '税率',
    `external_order_goods_id` bigint COMMENT '网站order_goods_id'
) COMMENT '来自kafka erp currency_conversion数据'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;


CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_order_goods (
    `rec_id` bigint COMMENT '',
    `order_id` bigint COMMENT '',
    `goods_id` bigint COMMENT '',
    `goods_name` string COMMENT '',
    `goods_sn` string COMMENT '',
    `goods_number` bigint COMMENT '',
    `market_price` decimal(13,4) COMMENT '',
    `goods_price` decimal(13,4) COMMENT '',
    `goods_attr` string COMMENT '',
    `send_number` string COMMENT '',
    `is_real` bigint COMMENT '',
    `extension_code` string COMMENT '',
    `parent_id` string COMMENT '',
    `is_gift` string COMMENT '',
    `goods_status` bigint COMMENT '',
    `action_amt` decimal(13,4) COMMENT '',
    `action_reason_cat` bigint COMMENT '',
    `action_note` string COMMENT '',
    `carrier_bill_id` bigint COMMENT '',
    `provider_id` bigint COMMENT '',
    `invoice_num` string COMMENT '',
    `return_points` bigint COMMENT '',
    `return_bonus` string COMMENT '',
    `biaoju_store_goods_id` bigint COMMENT '',
    `subtitle` string COMMENT '',
    `addtional_shipping_fee` bigint COMMENT '',
    `style_id` string COMMENT '',
    `customized` string COMMENT '',
    `status_id` string COMMENT '商品新旧状态',
    `added_fee` decimal(13,4) COMMENT '税率',
    `external_order_goods_id` bigint COMMENT '网站order_goods_id'
) COMMENT '来自对应arc表的数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

