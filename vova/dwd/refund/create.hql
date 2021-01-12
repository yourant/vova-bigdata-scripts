drop table dwd.dwd_vova_fact_refund;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd.dwd_vova_fact_refund
(
    datasource     string comment '数据平台',
    refund_id                      bigint COMMENT '退款ID',
    order_goods_id                 bigint COMMENT '子订单ID',
    refund_type_id                 bigint COMMENT '退款原因ID',
    risk_type                      string COMMENT '风控等级',
    refund_type                    string COMMENT '退款原因（一级）',
    refund_reason_type_id          bigint COMMENT '退款原因ID（二级）',
    refund_reason                  string COMMENT '退款原因（二级）',
    display_currency_id            bigint COMMENT '',
    order_currency_id              bigint COMMENT '',
    refund_wallet                  decimal(10, 4) COMMENT '主钱包退款金额',
    refund_amount                  decimal(10, 4) COMMENT '该退款的子订单金额',
    bonus                          decimal(10, 4) COMMENT '该退款的折扣，负值',
    refund_amount_exchange         decimal(10, 4) COMMENT '',
    bonus_exchange                 decimal(10, 4) COMMENT '该退款的折扣，订单货币，负值',
    display_refund_amount_exchange decimal(10, 4) COMMENT '',
    display_bonus_exchange         decimal(10, 4) COMMENT '',
    create_time                    timestamp COMMENT '创建时间',
    real_refund_amount_exchange    decimal(10, 4) COMMENT '',
    exec_refund_time               timestamp COMMENT '执行退款的时间',
    audit_status                   string COMMENT '退款审核状态',
    audit_time                     timestamp COMMENT '退款审核时间',
    sku_pay_status                 bigint COMMENT '支付状态',
    recheck_type                 bigint COMMENT '退款审核复核类型，0:默认，1:平台抽查，2:顾客申诉'
) COMMENT '退款事实表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


