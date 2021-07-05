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

# 取数代码
spark-sql -e "
select
    '订单下单时间','订单确认时间','订单ID','VOVA订单状态（0：未确认，1：已确认， 2 ：已取消）','自营店铺名','商品数量','一级分类','分类id','商品收入里所含运费（USD)','商品收入里所含商品价格(USD)(oldGroupPrice)','商品收入里所含商品价格(USD)(normalPrice)','运费-手动调整（USD）','销售价（USD）','gmv','商家结算价格','采购订单号','采购父订单号','供应链订单状态','采购订单状态','采购支付状态','采购配送状态','是否入库','采购价(RMB）','是否出库','尾程运单号','物流承运渠道','出库重量','预估运费','实际重量','实际运费'
union all
SELECT tnew.order_time,
       tnew.confirm_time,
       tnew.order_goods_sn,
       CASE
           WHEN sku_order_status = 0 THEN '未确认'
           WHEN sku_order_status = 1 THEN '已确认'
           WHEN sku_order_status = 2 THEN '已取消'
           WHEN sku_order_status = 5 THEN '已转移'
           ELSE sku_order_status END           AS sku_order_status,
       tnew.mct_name,
       tnew.goods_number,
       REPLACE(tnew.first_cat_name, ',', ' ')  AS first_cat_name,
       tnew.cat_id,
       tnew.shipping_price,
       tnew.display_old_group_price,
       tnew.display_normal_price,
       tnew.shipping_fee,
       tnew.display_price,
       tnew.order_amount,
       tnew.mct_order_amount,
       tnew.pdd_order_sn,
       tnew.pdd_parent_order_sn,
       CASE
           WHEN sales_order_status = 0 THEN '未确认'
           WHEN sales_order_status = 1 THEN '已确认'
           WHEN sales_order_status = 2 THEN '已取消'
           ELSE sales_order_status END         AS sales_order_status,
       CASE
           WHEN original_source = 'old' AND purchase_order_status = 0 THEN '未确认'
           WHEN original_source = 'old' AND purchase_order_status = 1 THEN '已确认'
           WHEN original_source = 'old' AND purchase_order_status = 2 THEN '已取消'
           WHEN original_source = 'old' AND purchase_order_status = 4 THEN '拍单失败'
           WHEN original_source = 'new' AND purchase_order_status = 0 THEN '未确认'
           WHEN original_source = 'new' AND purchase_order_status = 1 THEN '已确认'
           WHEN original_source = 'new' AND purchase_order_status = 2 THEN '已取消'
           WHEN original_source = 'new' AND purchase_order_status = 3 THEN '拍单失败'
           WHEN original_source = 'new' AND purchase_order_status = 4 THEN '拍单中'
           WHEN original_source = 'new' AND purchase_order_status = 5 THEN '已拍单'
           WHEN original_source = 'new' AND purchase_order_status = 6 THEN '已重新生成'
           WHEN original_source = 'new' AND purchase_order_status = 7 THEN '最终取消'
           ELSE purchase_order_status END      AS purchase_order_status,
       CASE
           WHEN original_source = 'new' AND purchase_pay_status = 0 THEN '未付款'
           WHEN original_source = 'new' AND purchase_pay_status = 1 THEN '未付款'
           WHEN original_source = 'new' AND purchase_pay_status = 2 THEN '已付款'
           WHEN original_source = 'new' AND purchase_pay_status = 3 THEN '已退款'
           WHEN original_source = 'new' AND purchase_pay_status = 4 THEN '待退款'
           WHEN original_source = 'new' AND purchase_pay_status = 5 THEN '审核不通过'
           WHEN original_source = 'new' AND purchase_pay_status = 6 THEN '待审核'
           WHEN original_source = 'old' AND purchase_pay_status = 0 THEN '未付款'
           WHEN original_source = 'old' AND purchase_pay_status = 1 THEN '付款中'
           WHEN original_source = 'old' AND purchase_pay_status = 2 THEN '已付款'
           WHEN original_source = 'old' AND purchase_pay_status = 4 THEN '已退款'
           WHEN original_source = 'old' AND purchase_pay_status = 30 THEN '退款待审核'
           ELSE purchase_pay_status END        AS purchase_pay_status,
       CASE
           WHEN original_source = 'new' AND purchase_shipping_status = 0 THEN '未发货'
           WHEN original_source = 'new' AND purchase_shipping_status = 1 THEN '未发货'
           WHEN original_source = 'new' AND purchase_shipping_status = 2 THEN '已发货'
           WHEN original_source = 'new' AND purchase_shipping_status = 3 THEN '已妥投'
           WHEN original_source = 'old' AND purchase_shipping_status = 0 THEN '未发货'
           WHEN original_source = 'old' AND purchase_shipping_status = 1 THEN '已发货'
           WHEN original_source = 'old' AND purchase_shipping_status = 2 THEN '已妥投'
           ELSE purchase_shipping_status END   AS purchase_shipping_status,
       CASE
           WHEN original_source = 'new' AND shipping_order_status = 0 THEN '未入库'
           WHEN original_source = 'new' AND shipping_order_status = 1 THEN '未入库'
           WHEN original_source = 'new' AND shipping_order_status = 2 THEN '入库成功'
           WHEN original_source = 'new' AND shipping_order_status = 3 THEN '入库失败'
           WHEN original_source = 'old' AND shipping_order_status = 0 THEN '初始状态未发送发货指令'
           WHEN original_source = 'old' AND shipping_order_status = 1 THEN '已发送发货指令'
           WHEN original_source = 'old' AND shipping_order_status = 2 THEN '货物出库成功'
           WHEN original_source = 'old' AND shipping_order_status = 3 THEN '货物出库失败/或者取消'
           ELSE shipping_order_status END AS shipping_order_status,
       tnew.purchase_total_amount,
       tnew.out_warehouse_status,
       tnew.last_waybill_no,
       tnew.carrier_code,
       tnew.guess_weight,
       tnew.plan_freight,
       tnew.actual_weight,
       tnew.actual_freight
FROM dwb.dwb_vova_financial_self_process tnew
WHERE date(tnew.order_time) <= '2020-05-31' and date(tnew.order_time) >= '2020-01-01'
    "  > /mnt/chenkai/exportfile/req4823_20210705.csv


