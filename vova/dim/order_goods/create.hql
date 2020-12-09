drop table dim.dim_vova_order_goods;
CREATE TABLE IF NOT EXISTS dim.dim_vova_order_goods
(
    datasource                            STRING COMMENT '数据平台',
    order_source                          STRING COMMENT '订单来源api,web',
    platform                              STRING COMMENT '买家平台',
    device_id                             STRING COMMENT '买家设备ID',
    from_domain                           STRING COMMENT 'domain来源',
    buyer_id                              BIGINT COMMENT '买家ID',
    order_id                              BIGINT COMMENT '订单ID',
    order_sn                              STRING COMMENT 'oi.order_sn',
    coupon_code                           STRING COMMENT '优惠券CODE',
    gender                                STRING COMMENT '买家性别',
    email                                 STRING COMMENT '买家联系email',
    parent_order_id                       BIGINT COMMENT 'oi.parent_order_id',
    payment_id                            BIGINT COMMENT '支付方式ID',
    payment_name                          STRING COMMENT '支付方式名字',
    order_currency_id                     BIGINT COMMENT '货币ID',
    base_currency_id                      BIGINT COMMENT '货币ID',
    order_time                            TIMESTAMP COMMENT '下单时间',
    pay_time                              TIMESTAMP COMMENT '支付时间',
    receive_time                          TIMESTAMP COMMENT '收款时间',
    pay_status                            BIGINT COMMENT 'oi.pay_status',
    region_id                             BIGINT COMMENT '订单国家ID',
    region_code                           STRING COMMENT '订单国家code',
    order_goods_id                        BIGINT COMMENT '子订单ID',
    order_goods_sn                        STRING COMMENT 'og.order_goods_sn',
    parent_rec_id                         BIGINT COMMENT '克隆子订单ID',
    goods_id                              BIGINT COMMENT '商品ID',
    goods_sn                              STRING COMMENT '商品SN',
    goods_name                            STRING COMMENT '商品NAME',
    sku_id                                BIGINT COMMENT 'og.sku_id',
    goods_number                          BIGINT COMMENT 'og.goods_number',
    shop_price                            DECIMAL(14, 4) COMMENT 'og.shop_price',
    shipping_fee                          DECIMAL(14, 4) COMMENT 'og.shipping_fee',
    goods_weight                          DECIMAL(14, 4) COMMENT 'og.goods_weight',
    bonus                                 DECIMAL(14, 4) COMMENT 'og.bonus',
    mct_shop_price                        DECIMAL(14, 4) COMMENT 'og.mct_shop_price',
    mct_shipping_fee                      DECIMAL(14, 4) COMMENT 'og.mct_shipping_fee',
    sku_order_status                      BIGINT COMMENT '订单状态',
    sku_pay_status                        BIGINT COMMENT '支付状态',
    sku_shipping_status                   BIGINT COMMENT '发货状态',
    sku_collecting_status                 BIGINT COMMENT 'ogs.sku_collecting_status',
    confirm_time                          TIMESTAMP COMMENT '订单确认时间',
    shipping_time                         TIMESTAMP COMMENT '发货时间',
    collecting_time                       TIMESTAMP COMMENT '集货仓发货时间',
    cat_id                                BIGINT COMMENT '商品类别',
    virtual_goods_id                      BIGINT COMMENT '商品虚拟id',
    first_cat_name                        STRING COMMENT '商品一级分类',
    second_cat_name                       STRING COMMENT '商品二级分类',
    brand_id                              BIGINT COMMENT '商品brand_id',
    mct_id                                BIGINT COMMENT '商品卖家ID',
    order_tag                             STRING COMMENT '订单标签',
    order_goods_tag                       STRING COMMENT '子订单标签',
    delivery_time                         TIMESTAMP COMMENT '交期时间',
    delivery_time_max                     TIMESTAMP COMMENT '最大交期时间',
    lgst_way                              STRING COMMENT '物流方式, is_fbv是海外仓发货, not_fbv',
    collection_plan_id                    BIGINT COMMENT '集货计划id',
    container_transportation_shipping_fee STRING COMMENT '集运增值运费'
) COMMENT '子订单维度'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
