create table if not exists dwd.dwd_fd_ecs_order_info_paid
(
    ecs_order_id           bigint comment 'ecs订单id',
    order_sn               string comment '和网站订单关联',
    party_id               bigint comment '组织id',
    project                string comment '组织名称',
    order_time             timestamp comment 'ecs订单生成时间,UTC',
    country_id             bigint comment '国家id',
    country_code           string comment '国家代码',
    user_id                bigint comment '用户id',
    goods_amount           DECIMAL(15, 4) comment '商品销售额USD',
    bouns_amount           DECIMAL(15, 4) comment '优惠金额USD',
    ads_cost               DECIMAL(15, 4) comment '均摊广告花费USD',
    es_purchase_amount     DECIMAL(15, 4) comment '采购花费-订单和发货USD',
    purchase_amount        DECIMAL(15, 4) comment '采购花费USD',
    goods_refund_amount    DECIMAL(15, 4) comment '商品-退款金额USD',
    shipping_refund_amount DECIMAL(15, 4) comment '运费-退款金额USD',
    ecs_order_amount       DECIMAL(15, 4) comment 'ecs订单总金额USD，受退货影响',
    vb_order_amount        DECIMAL(15, 4) comment '下单时的订单总金额USD',
    order_amount_diff      DECIMAL(15, 4) comment '订单金额和实际收款差值,vb_order_amount-ecs_order_amount'
) comment 'ecs已支付的订单'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS PARQUET;


create table if not exists dwd.dwd_fd_ecs_order_info_shipping
(
    ecs_order_id           bigint comment 'ecs订单id',
    order_sn               string comment '和网站订单关联',
    party_id               bigint comment '组织id',
    project                string comment '组织名称',
    order_time             timestamp comment 'ecs订单生成时间,UTC',
    shipping_time          timestamp comment '发货时间,UTC',
    country_id             bigint comment '国家id',
    country_code           string comment '国家代码',
    user_id                bigint comment '用户id',
    goods_amount           DECIMAL(15, 4) comment '商品销售额USD',
    bouns_amount           DECIMAL(15, 4) comment '优惠金额USD',
    ads_cost               DECIMAL(15, 4) comment '均摊广告花费USD',
    es_purchase_amount     DECIMAL(15, 4) comment '采购花费-订单和发货USD',
    purchase_amount        DECIMAL(15, 4) comment '采购花费USD',
    goods_refund_amount    DECIMAL(15, 4) comment '商品-退款金额USD',
    shipping_refund_amount DECIMAL(15, 4) comment '运费-退款金额USD',
    ecs_order_amount       DECIMAL(15, 4) comment 'ecs订单总金额USD，受退货影响',
    vb_order_amount        DECIMAL(15, 4) comment '下单时的订单总金额USD',
    order_amount_diff      DECIMAL(15, 4) comment '订单金额和实际收款差值,vb_order_amount-ecs_order_amount'
) comment 'ecs已支付并且发货的订单'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS PARQUET;