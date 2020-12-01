CREATE TABLE IF NOT EXISTS dwb.dwb_fd_order_coupon_rpt
(
    project_name     string comment '组织',
    coupon_code      string COMMENT '优惠券code',
    order_id 	     bigint COMMENT '订单id',
    order_sn         string COMMENT '订单SN',
    order_amount     DECIMAL(15, 4) COMMENT '订单金额',
    gmv              DECIMAL(15, 4) COMMENT 'GMV',
    shipping_fee     DECIMAL(15, 4) COMMENT '运费',
    true_bonus       DECIMAL(15, 4) COMMENT '优惠金额',
    order_time_utc   string comment '订单utc时间',
    order_time_pst   string COMMENT '订单洛杉矶时间',
    order_status     bigint COMMENT '订单状态',
    pay_status       bigint COMMENT '订单支付状态',
    platform_type    string comment '平台',
    country_code     string COMMENT '国家code',
    language_code    string COMMENT '语言code'
) COMMENT '订单优惠券信息表'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC;