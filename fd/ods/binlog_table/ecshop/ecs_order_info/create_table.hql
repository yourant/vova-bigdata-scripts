CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_order_info_inc (
    `order_id` bigint COMMENT '',
    `party_id` bigint COMMENT '',
    `order_sn` string COMMENT '',
    `user_id` string COMMENT '',
    `order_time` timestamp COMMENT '',
    `order_status` bigint COMMENT '',
    `shipping_status` bigint COMMENT '',
    `pay_status` bigint COMMENT '',
    `consignee` string COMMENT '',
    `sex` string COMMENT '',
    `country` string COMMENT '',
    `province` string COMMENT '',
    `city` string COMMENT '',
    `district` string COMMENT '',
    `address` string COMMENT '',
    `zipcode` string COMMENT '',
    `tel` string COMMENT '',
    `mobile` string COMMENT '',
    `email` string COMMENT '',
    `best_time` string COMMENT '',
    `sign_building` string COMMENT '',
    `postscript` string COMMENT '',
    `shipping_id` bigint COMMENT '',
    `shipping_name` string COMMENT '',
    `pay_id` bigint COMMENT '',
    `pay_name` string COMMENT '',
    `how_oos` string COMMENT '',
    `how_surplus` string COMMENT '',
    `pack_name` string COMMENT '',
    `card_name` string COMMENT '',
    `card_message` string COMMENT '',
    `inv_payee` string COMMENT '',
    `inv_content` string COMMENT '',
    `inv_address` string COMMENT '',
    `inv_zipcode` string COMMENT '',
    `inv_phone` string COMMENT '',
    `goods_amount` decimal(13,4) COMMENT '',
    `shipping_fee` decimal(13,4) COMMENT '',
    `duty_fee` decimal(13,4) COMMENT '',
    `insure_fee` decimal(13,4) COMMENT '',
    `shipping_proxy_fee` decimal(13,4) COMMENT '',
    `pay_fee` decimal(13,4) COMMENT '',
    `pack_fee` decimal(13,4) COMMENT '',
    `card_fee` decimal(13,4) COMMENT '',
    `money_paid` decimal(13,4) COMMENT '',
    `surplus` decimal(13,4) COMMENT '',
    `integral` bigint COMMENT '',
    `integral_money` decimal(13,4) COMMENT '',
    `bonus` decimal(13,4) COMMENT '',
    `order_amount` decimal(13,4) COMMENT '',
    `from_ad` string COMMENT '',
    `referer` string COMMENT '',
    `confirm_time` bigint COMMENT '',
    `pay_time` bigint COMMENT '',
    `shipping_time` bigint COMMENT '',
    `reserved_time` bigint COMMENT '预订时间',
    `pack_id` bigint COMMENT '',
    `card_id` bigint COMMENT '',
    `bonus_id` string COMMENT '',
    `invoice_no` string COMMENT '',
    `extension_code` string COMMENT '',
    `extension_id` string COMMENT '',
    `to_buyer` string COMMENT '',
    `pay_note` string COMMENT '',
    `invoice_status` bigint COMMENT '',
    `carrier_bill_id` bigint COMMENT '',
    `receiving_time` bigint COMMENT '',
    `biaoju_store_id` bigint COMMENT '',
    `parent_order_id` bigint COMMENT '',
    `track_id` string COMMENT '',
    `real_paid` decimal(13,4) COMMENT '',
    `real_shipping_fee` decimal(13,4) COMMENT '',
    `is_shipping_fee_clear` bigint COMMENT '',
    `is_order_amount_clear` bigint COMMENT '',
    `is_ship_emailed` bigint COMMENT '',
    `proxy_amount` decimal(13,4) COMMENT '',
    `pay_method` string COMMENT '',
    `is_back` string COMMENT '',
    `is_finance_clear` bigint COMMENT '',
    `finance_clear_type` bigint COMMENT '',
    `handle_time` bigint COMMENT '',
    `start_shipping_time` bigint COMMENT '',
    `end_shipping_time` bigint COMMENT '',
    `shortage_status` bigint COMMENT '',
    `is_shortage_await` string COMMENT '缺货等待',
    `order_type_id` string COMMENT '订单类型',
    `is_display` string COMMENT '是否显示给客服',
    `special_type_id` string COMMENT '特殊类型定义',
    `misc_fee` decimal(13,4) COMMENT '杂项费用',
    `additional_amount` decimal(13,4) COMMENT '-h订单还需支付金额',
    `distributor_id` string COMMENT '',
    `taobao_order_sn` string COMMENT '',
    `distribution_purchase_order_sn` string COMMENT '分销采购单号',
    `need_invoice` string COMMENT '是否需要发票',
    `facility_id` string COMMENT '仓库id',
    `currency` string COMMENT '',
    `language_id` string COMMENT '',
    `track_status` string COMMENT '订单跟踪状态，T代表跟踪中，F代表跟踪完成',
    `defray_time` timestamp COMMENT '字段当前用作order_action首次变为付款时间，以后考虑用作网站客户真实付款时间(北京)'
) COMMENT '来自kafka erp订单每日增量数据'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_order_info_arc (
    `order_id` bigint COMMENT '',
    `party_id` bigint COMMENT '',
    `order_sn` string COMMENT '',
    `user_id` string COMMENT '',
    `order_time` timestamp COMMENT '',
    `order_status` bigint COMMENT '',
    `shipping_status` bigint COMMENT '',
    `pay_status` bigint COMMENT '',
    `consignee` string COMMENT '',
    `sex` string COMMENT '',
    `country` string COMMENT '',
    `province` string COMMENT '',
    `city` string COMMENT '',
    `district` string COMMENT '',
    `address` string COMMENT '',
    `zipcode` string COMMENT '',
    `tel` string COMMENT '',
    `mobile` string COMMENT '',
    `email` string COMMENT '',
    `best_time` string COMMENT '',
    `sign_building` string COMMENT '',
    `postscript` string COMMENT '',
    `shipping_id` bigint COMMENT '',
    `shipping_name` string COMMENT '',
    `pay_id` bigint COMMENT '',
    `pay_name` string COMMENT '',
    `how_oos` string COMMENT '',
    `how_surplus` string COMMENT '',
    `pack_name` string COMMENT '',
    `card_name` string COMMENT '',
    `card_message` string COMMENT '',
    `inv_payee` string COMMENT '',
    `inv_content` string COMMENT '',
    `inv_address` string COMMENT '',
    `inv_zipcode` string COMMENT '',
    `inv_phone` string COMMENT '',
    `goods_amount` decimal(13,4) COMMENT '',
    `shipping_fee` decimal(13,4) COMMENT '',
    `duty_fee` decimal(13,4) COMMENT '',
    `insure_fee` decimal(13,4) COMMENT '',
    `shipping_proxy_fee` decimal(13,4) COMMENT '',
    `pay_fee` decimal(13,4) COMMENT '',
    `pack_fee` decimal(13,4) COMMENT '',
    `card_fee` decimal(13,4) COMMENT '',
    `money_paid` decimal(13,4) COMMENT '',
    `surplus` decimal(13,4) COMMENT '',
    `integral` bigint COMMENT '',
    `integral_money` decimal(13,4) COMMENT '',
    `bonus` decimal(13,4) COMMENT '',
    `order_amount` decimal(13,4) COMMENT '',
    `from_ad` string COMMENT '',
    `referer` string COMMENT '',
    `confirm_time` bigint COMMENT '',
    `pay_time` bigint COMMENT '',
    `shipping_time` bigint COMMENT '',
    `reserved_time` bigint COMMENT '预订时间',
    `pack_id` bigint COMMENT '',
    `card_id` bigint COMMENT '',
    `bonus_id` string COMMENT '',
    `invoice_no` string COMMENT '',
    `extension_code` string COMMENT '',
    `extension_id` string COMMENT '',
    `to_buyer` string COMMENT '',
    `pay_note` string COMMENT '',
    `invoice_status` bigint COMMENT '',
    `carrier_bill_id` bigint COMMENT '',
    `receiving_time` bigint COMMENT '',
    `biaoju_store_id` bigint COMMENT '',
    `parent_order_id` bigint COMMENT '',
    `track_id` string COMMENT '',
    `real_paid` decimal(13,4) COMMENT '',
    `real_shipping_fee` decimal(13,4) COMMENT '',
    `is_shipping_fee_clear` bigint COMMENT '',
    `is_order_amount_clear` bigint COMMENT '',
    `is_ship_emailed` bigint COMMENT '',
    `proxy_amount` decimal(13,4) COMMENT '',
    `pay_method` string COMMENT '',
    `is_back` string COMMENT '',
    `is_finance_clear` bigint COMMENT '',
    `finance_clear_type` bigint COMMENT '',
    `handle_time` bigint COMMENT '',
    `start_shipping_time` bigint COMMENT '',
    `end_shipping_time` bigint COMMENT '',
    `shortage_status` bigint COMMENT '',
    `is_shortage_await` string COMMENT '缺货等待',
    `order_type_id` string COMMENT '订单类型',
    `is_display` string COMMENT '是否显示给客服',
    `special_type_id` string COMMENT '特殊类型定义',
    `misc_fee` decimal(13,4) COMMENT '杂项费用',
    `additional_amount` decimal(13,4) COMMENT '-h订单还需支付金额',
    `distributor_id` string COMMENT '',
    `taobao_order_sn` string COMMENT '',
    `distribution_purchase_order_sn` string COMMENT '分销采购单号',
    `need_invoice` string COMMENT '是否需要发票',
    `facility_id` string COMMENT '仓库id',
    `currency` string COMMENT '',
    `language_id` string COMMENT '',
    `track_status` string COMMENT '订单跟踪状态，T代表跟踪中，F代表跟踪完成',
    `defray_time` timestamp COMMENT '字段当前用作order_action首次变为付款时间，以后考虑用作网站客户真实付款时间(北京)'
) COMMENT '来自kafka erp订单每日增量数据'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_order_info (
    `order_id` bigint COMMENT '',
    `party_id` bigint COMMENT '',
    `order_sn` string COMMENT '',
    `user_id` string COMMENT '',
    `order_time` timestamp COMMENT '',
    `order_status` bigint COMMENT '',
    `shipping_status` bigint COMMENT '',
    `pay_status` bigint COMMENT '',
    `consignee` string COMMENT '',
    `sex` string COMMENT '',
    `country` string COMMENT '',
    `province` string COMMENT '',
    `city` string COMMENT '',
    `district` string COMMENT '',
    `address` string COMMENT '',
    `zipcode` string COMMENT '',
    `tel` string COMMENT '',
    `mobile` string COMMENT '',
    `email` string COMMENT '',
    `best_time` string COMMENT '',
    `sign_building` string COMMENT '',
    `postscript` string COMMENT '',
    `shipping_id` bigint COMMENT '',
    `shipping_name` string COMMENT '',
    `pay_id` bigint COMMENT '',
    `pay_name` string COMMENT '',
    `how_oos` string COMMENT '',
    `how_surplus` string COMMENT '',
    `pack_name` string COMMENT '',
    `card_name` string COMMENT '',
    `card_message` string COMMENT '',
    `inv_payee` string COMMENT '',
    `inv_content` string COMMENT '',
    `inv_address` string COMMENT '',
    `inv_zipcode` string COMMENT '',
    `inv_phone` string COMMENT '',
    `goods_amount` decimal(13,4) COMMENT '',
    `shipping_fee` decimal(13,4) COMMENT '',
    `duty_fee` decimal(13,4) COMMENT '',
    `insure_fee` decimal(13,4) COMMENT '',
    `shipping_proxy_fee` decimal(13,4) COMMENT '',
    `pay_fee` decimal(13,4) COMMENT '',
    `pack_fee` decimal(13,4) COMMENT '',
    `card_fee` decimal(13,4) COMMENT '',
    `money_paid` decimal(13,4) COMMENT '',
    `surplus` decimal(13,4) COMMENT '',
    `integral` bigint COMMENT '',
    `integral_money` decimal(13,4) COMMENT '',
    `bonus` decimal(13,4) COMMENT '',
    `order_amount` decimal(13,4) COMMENT '',
    `from_ad` string COMMENT '',
    `referer` string COMMENT '',
    `confirm_time` bigint COMMENT '',
    `pay_time` bigint COMMENT '',
    `shipping_time` bigint COMMENT '',
    `reserved_time` bigint COMMENT '预订时间',
    `pack_id` bigint COMMENT '',
    `card_id` bigint COMMENT '',
    `bonus_id` string COMMENT '',
    `invoice_no` string COMMENT '',
    `extension_code` string COMMENT '',
    `extension_id` string COMMENT '',
    `to_buyer` string COMMENT '',
    `pay_note` string COMMENT '',
    `invoice_status` bigint COMMENT '',
    `carrier_bill_id` bigint COMMENT '',
    `receiving_time` bigint COMMENT '',
    `biaoju_store_id` bigint COMMENT '',
    `parent_order_id` bigint COMMENT '',
    `track_id` string COMMENT '',
    `real_paid` decimal(13,4) COMMENT '',
    `real_shipping_fee` decimal(13,4) COMMENT '',
    `is_shipping_fee_clear` bigint COMMENT '',
    `is_order_amount_clear` bigint COMMENT '',
    `is_ship_emailed` bigint COMMENT '',
    `proxy_amount` decimal(13,4) COMMENT '',
    `pay_method` string COMMENT '',
    `is_back` string COMMENT '',
    `is_finance_clear` bigint COMMENT '',
    `finance_clear_type` bigint COMMENT '',
    `handle_time` bigint COMMENT '',
    `start_shipping_time` bigint COMMENT '',
    `end_shipping_time` bigint COMMENT '',
    `shortage_status` bigint COMMENT '',
    `is_shortage_await` string COMMENT '缺货等待',
    `order_type_id` string COMMENT '订单类型',
    `is_display` string COMMENT '是否显示给客服',
    `special_type_id` string COMMENT '特殊类型定义',
    `misc_fee` decimal(13,4) COMMENT '杂项费用',
    `additional_amount` decimal(13,4) COMMENT '-h订单还需支付金额',
    `distributor_id` string COMMENT '',
    `taobao_order_sn` string COMMENT '',
    `distribution_purchase_order_sn` string COMMENT '分销采购单号',
    `need_invoice` string COMMENT '是否需要发票',
    `facility_id` string COMMENT '仓库id',
    `currency` string COMMENT '',
    `language_id` string COMMENT '',
    `track_status` string COMMENT '订单跟踪状态，T代表跟踪中，F代表跟踪完成',
    `defray_time` timestamp COMMENT '字段当前用作order_action首次变为付款时间，以后考虑用作网站客户真实付款时间(北京)'
) COMMENT '来自对应arc表的订单数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;
