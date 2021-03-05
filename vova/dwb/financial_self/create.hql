DROP TABLE dwb.dwb_vova_financial_self;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_financial_self
(
    original_source          string COMMENT 'i_数据来源（新老中台）',
    order_time               TIMESTAMP COMMENT 'i_订单下单时间',
    confirm_time             TIMESTAMP COMMENT 'i_订单确认时间',
    order_goods_sn           string COMMENT 'i_订单ID',
    sku_order_status         string COMMENT 'i_VOVA订单状态（0：未确认，1：已确认， 2 ：已取消）',
    mct_name                 string COMMENT 'i_自营店铺名',
    goods_number             bigint COMMENT 'i_商品数量',
    first_cat_name           string COMMENT 'i_一级分类',
    cat_id                   bigint COMMENT 'i_分类id',
    shipping_price           decimal(15, 2) COMMENT 'i_商品收入里所含运费（USD)',
    display_old_group_price  decimal(15, 2) COMMENT 'i_商品收入里所含商品价格(USD)(oldGroupPrice)',
    display_normal_price     decimal(15, 2) COMMENT 'i_商品收入里所含商品价格(USD)(normalPrice)',
    shipping_fee             decimal(15, 2) COMMENT 'i_运费-手动调整（USD）',
    display_price            decimal(15, 2) COMMENT 'i_销售价（USD）',
    order_amount             decimal(15, 2) COMMENT 'i_gmv',
    mct_order_amount         decimal(15, 2) COMMENT 'i_商家结算价格',
    pdd_order_sn             string COMMENT 'i_采购订单号',
    pdd_parent_order_sn      string COMMENT 'i_采购父订单号',
    sales_order_status       string COMMENT 'i_供应链订单状态',
    purchase_order_status    string COMMENT 'i_采购订单状态',
    purchase_pay_status      string COMMENT 'i_采购支付状态',
    purchase_shipping_status string COMMENT 'i_采购配送状态',
    shipping_order_status    string COMMENT 'i_是否入库',
    purchase_total_amount    decimal(15, 2) COMMENT 'i_采购价(RMB）',
    out_warehouse_status     string COMMENT 'i_是否出库',
    last_waybill_no          string COMMENT 'i_尾程运单号',
    carrier_code     string COMMENT 'i_物流承运渠道',
    guess_weight     decimal(10, 3) COMMENT 'i_出库重量',
    plan_freight     decimal(10, 2) COMMENT 'i_预估运费',
    actual_weight     decimal(10, 3) COMMENT 'i_实际重量',
    actual_freight     decimal(10, 2) COMMENT 'i_实际运费'
) COMMENT '自营店铺销售采购明细' PARTITIONED BY (pt STRING)
   STORED AS PARQUETFILE;

DROP TABLE dwb.dwb_vova_financial_self_process;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_financial_self_process
(
    original_source          string COMMENT 'i_数据来源（新老中台）',
    order_time               TIMESTAMP COMMENT 'i_订单下单时间',
    confirm_time             TIMESTAMP COMMENT 'i_订单确认时间',
    order_goods_sn           string COMMENT 'i_订单ID',
    sku_order_status         bigint COMMENT 'i_VOVA订单状态（0：未确认，1：已确认， 2 ：已取消）',
    mct_name                 string COMMENT 'i_自营店铺名',
    goods_number             bigint COMMENT 'i_商品数量',
    first_cat_name           string COMMENT 'i_一级分类',
    cat_id                   bigint COMMENT 'i_分类id',
    shipping_price           decimal(15, 2) COMMENT 'i_商品收入里所含运费（USD)',
    normal_price             decimal(15, 2) COMMENT '',
    old_group_price          decimal(15, 2) COMMENT '',
    shipping_fee             decimal(15, 2) COMMENT 'i_运费-手动调整（USD）',
    order_amount             decimal(15, 2) COMMENT 'i_gmv',
    mct_order_amount         decimal(15, 2) COMMENT 'i_商家结算价格',
    pdd_order_sn             string COMMENT 'i_采购订单号',
    pdd_parent_order_sn      string COMMENT 'i_采购父订单号',
    sales_order_status       bigint COMMENT 'i_供应链订单状态',
    purchase_order_status    bigint COMMENT 'i_采购订单状态',
    purchase_pay_status      bigint COMMENT 'i_采购支付状态',
    purchase_shipping_status bigint COMMENT 'i_采购配送状态',
    shipping_order_status    bigint COMMENT 'i_是否入库',
    purchase_total_amount    decimal(15, 2) COMMENT 'i_采购价(RMB）',
    display_normal_price     decimal(15, 2) COMMENT 'i_商品收入里所含商品价格(USD)(normalPrice)',
    display_old_group_price  decimal(15, 2) COMMENT 'i_商品收入里所含商品价格(USD)(oldGroupPrice)',
    display_price            decimal(15, 2) COMMENT 'i_销售价（USD）',
    out_warehouse_status     string COMMENT 'i_是否出库',
    last_waybill_no          string COMMENT 'i_尾程运单号',
    carrier_code     string COMMENT 'i_物流承运渠道',
    guess_weight     decimal(10, 3) COMMENT 'i_出库重量',
    plan_freight     decimal(10, 2) COMMENT 'i_预估运费',
    actual_weight     decimal(10, 3) COMMENT 'i_实际重量',
    actual_freight     decimal(10, 2) COMMENT 'i_实际运费'
) COMMENT '自营店铺销售采购明细' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE;