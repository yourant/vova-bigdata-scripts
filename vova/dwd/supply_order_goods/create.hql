DROP TABLE dwd.dwd_vova_fact_supply_order_goods;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd.dwd_vova_fact_supply_order_goods
(
    order_goods_id                    bigint COMMENT '销售子订单id',
    order_id                          bigint COMMENT '销售父订单id',
    channel_order_goods_sn            string COMMENT 'vova.order_goods.order_goods_sn',
    last_waybill_no                   string COMMENT '同vova.ost.shipping_tracking_number',
    order_goods_shipping_status    bigint COMMENT '同og.order_goods_shipping_status',
    purchase_platform_order_id        string COMMENT 'pdd_order_sn',
    purchase_platform_parent_order_id string COMMENT 'pdd_parent_order_sn',
    purchase_order_goods_id           bigint COMMENT '采购子订单id',
    purchase_order_status             bigint COMMENT '采购订单状态',
    purchase_order_pay_status         bigint COMMENT '采购支付状态',
    purchase_order_shipping_status    bigint COMMENT '采购发货状态',
    in_inventory_status               bigint COMMENT '采购入库状态',
    purchase_amount                   DECIMAL(14, 2) COMMENT '采购金额',
    purchase_order_id                 bigint COMMENT '采购父订单id',
    purchase_shipping_time            TIMESTAMP COMMENT '采购订单发货时间',
    inbound_weight                    DECIMAL(10, 3) COMMENT '入库重量单位g,订单粒度',
    bound_status                      bigint COMMENT '出入库状态;未入库1，已入库10,订单粒度',
    purchase_waybill_no               string COMMENT '采购渠道运单号',
    plan_freight                      DECIMAL(10, 2) COMMENT '预计运费,运单粒度',
    actual_freight                    DECIMAL(10, 2) COMMENT '实际运费,运单粒度',
    actual_weight                     DECIMAL(10, 3) COMMENT '实际包裹重量 单位g,运单粒度',
    guess_weight                      DECIMAL(10, 3) COMMENT '出库重量,运单粒度',
    carrier_code                      string COMMENT '承运商',
    refer_waybill_no                  string COMMENT '销售参考运单'
) COMMENT '供应链订单事实表' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
