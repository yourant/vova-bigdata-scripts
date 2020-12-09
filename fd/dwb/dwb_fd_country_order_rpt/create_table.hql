CREATE TABLE IF NOT EXISTS dwb.dwb_fd_country_order_rpt
(
    project_name     string comment '组织',
    platform         string COMMENT '平台',
    country_code     string COMMENT '国家',
    pay_status       string COMMENT '支付状态',
    orders           bigint COMMENT '订单数',
    order_amount_free  DECIMAL(15, 4) COMMENT 'order_amount_free',
    order_amount       DECIMAL(15, 4) COMMENT 'order_amount',
    users              bigint COMMENT '用户数',
    customer_price_free   DECIMAL(15, 4) comment 'customer_price_free',
    customer_price      DECIMAL(15, 4) COMMENT 'customer_price',
    shipping_free       DECIMAL(15, 4) COMMENT 'shipping_free'
) COMMENT '国家订单相关金额报表'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");