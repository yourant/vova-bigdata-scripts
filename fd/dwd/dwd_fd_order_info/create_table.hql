CREATE TABLE IF NOT EXISTS dwd.dwd_fd_order_info (
    sp_duid string COMMENT '来自打点数据',
    order_id bigint COMMENT '订单id',
    party_id bigint COMMENT 'party_id',
    order_sn string COMMENT '订单SN',
    user_id bigint COMMENT '用户id',
    order_time_original timestamp COMMENT '转化之前的值 订单时间-洛杉矶',
    order_time bigint COMMENT '订单时间',
    order_status bigint COMMENT '订单状态',
    shipping_status bigint COMMENT '物流状态',
    pay_status bigint COMMENT '支付状态',
    country_id bigint COMMENT '国家id',
    mobile string COMMENT '手机',
    email string COMMENT '邮箱',
    payment_id bigint COMMENT '支付id',
    payment_name string COMMENT '支付名',
    goods_amount decimal(15, 4) COMMENT '商品金额',
    goods_amount_exchange decimal(15, 4) COMMENT '商品转换后的数额',
    shipping_fee decimal(15, 4) COMMENT '运费',
    shipping_fee_exchange decimal(15, 4) COMMENT '运费转换后的数额',
    integral bigint COMMENT '已经抵用欧币',
    integral_money decimal(15, 4) COMMENT '积分',
    bonus decimal(15, 4) COMMENT '优惠费用，负值',
    bonus_exchange decimal(15, 4),
    order_amount decimal(15, 4) COMMENT '订单金额',
    base_currency_id bigint COMMENT '币种ID',
    order_currency_id bigint COMMENT '生成订单时用户选择的币种',
    order_currency_symbol string COMMENT 'like US$ HK$',
    rate string COMMENT '字符串：exchange/base',
    order_amount_exchange decimal(15, 4) COMMENT '转换后的数额',
    from_ad bigint COMMENT '',
    referer string COMMENT '',
    pay_time_original timestamp COMMENT '转化之前的值-洛杉矶',
    pay_time bigint COMMENT '支付时间',
    coupon_code string COMMENT '优惠券代码',
    order_type_id string,
    taobao_order_sn string COMMENT '和erp系统订单相关',
    language_id bigint COMMENT '语言id',
    coupon_cat_id bigint COMMENT '',
    coupon_config_value decimal(15, 4) COMMENT '@see ok_coupon_config',
    coupon_config_coupon_type string COMMENT '@see ok_coupon_config',
    is_conversion bigint COMMENT '数据是否已提交给google adwords',
    from_domain string COMMENT '订单来源',
    project_name string COMMENT '组织名/站点',
    user_agent_id bigint COMMENT '下单时的 user agent',
    platform_type string COMMENT '平台',
    version string COMMENT 'APP版本号',
    is_app bigint COMMENT '是否APP',
    device_type string COMMENT '设备类型',
    os_type string COMMENT '操作系统类型',
    country_code string COMMENT '国家code',
    language_code string COMMENT '语言code',
    order_currency_code string COMMENT '货币单位'
) COMMENT '订单事实表数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;