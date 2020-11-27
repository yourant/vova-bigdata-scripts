CREATE TABLE IF NOT EXISTS dwb.dwb_fd_rpt_ecommerce_funnel_report
(
    project            STRING comment '网站名称',
    country            STRING comment '国家',
    platform_type      STRING comment '平台类型',
    ga_channel         STRING comment 'session来源渠道',
    is_new_user        STRING comment '是否新会话',
    mkt_source         STRING comment '广告来源',
    mkt_medium         STRING comment '广告投放方式',
    campaign_name      STRING comment '广告账户名称',

    all_uv             BIGINT comment '总会话',
    product_uv         BIGINT comment '详情页总会话',
    add_uv             BIGINT comment '加车总会话',
    checkout_uv        BIGINT comment 'checkout总会话',
    checkout_option_uv BIGINT comment '下单总会话',
    purchase_uv        BIGINT comment '完成订单总会话',

    paid_order_num     BIGINT comment '支付成功的订单数',
    sum_goods_amount   DECIMAL(15, 4) comment '总订单商品金额',
    sum_shipping_fee   DECIMAL(15, 4) comment '总订单运费'

) comment '网站转化漏斗报表'
    PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS ORC;