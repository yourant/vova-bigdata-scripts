自营店铺退款率报表

需求方及需求号：#4435 , 财务
创建时间及开发人员：2020-06-05, 郑智宇

dwb.dwb_vova_financial_self_process

DROP TABLE dwb.dwb_vova_finance_self_merchant_refund;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_finance_self_merchant_refund
(
    event_date                                       date COMMENT 'd_日期',
    mct_name                                         string COMMENT 'd_商家',
    confirm_order_cnt                                bigint COMMENT 'i_订单确认单量',
    confirm_mct_amount                               DECIMAL(14, 2) COMMENT 'i_订单确认金额',
    cancel_order_cnt                                 bigint COMMENT 'i_发货前取消单量',
    cancel_mct_amount                                DECIMAL(14, 2) COMMENT 'i_发货前取消金额',
    cancel_purchase_order_cnt                        bigint COMMENT 'i_售中退款前已采购单量',
    cancel_purchase_mct_amount                       DECIMAL(14, 2) COMMENT 'i_售中退款前已采购的订单金额',
    cancel_purchase_total_amount                     DECIMAL(14, 2) COMMENT 'i_售中退款前已采购成本',
    shipping_order_cnt                               bigint COMMENT 'i_已发货单量',
    shipping_mct_amount                              DECIMAL(14, 2) COMMENT 'i_已发货金额',
    not_shipping_order_cnt                           bigint COMMENT 'i_未发货单量',
    not_shipping_mct_amount                          DECIMAL(14, 2) COMMENT 'i_未发货金额',
    cancel_shipping_order_cnt                        bigint COMMENT 'i_售后退款单量',
    cancel_shipping_mct_amount                       DECIMAL(14, 2) COMMENT 'i_售后退款金额',
    cancel_order_cnt_div_confirm_order_cnt           DECIMAL(14, 4) COMMENT 'i_广义售中退款率（按单量）',
    cancel_purchase_order_cnt_div_confirm_order_cnt  DECIMAL(14, 4) COMMENT 'i_狭义售中退款率（按单量）',
    shipping_order_cnt_div_confirm_order_cnt_b       DECIMAL(14, 4) COMMENT 'i_完成发货率（按单量）',
    cancel_shipping_order_cnt_div_shipping_order_cnt DECIMAL(14, 4) COMMENT 'i_售后退款率（按单量）'
) COMMENT '自营店铺退款率报表' PARTITIONED BY ( pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_finance_self_merchant_refund/"
;


DROP TABLE dwb.dwb_vova_finance_self_merchant_refund_detail;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_finance_self_merchant_refund_detail
(
    event_date                                       date COMMENT 'd_日期',
    mct_name                                         string COMMENT 'd_商家',
    refund_type                                      string COMMENT 'i_退款原因',
    refund_order_cnt                                 bigint COMMENT 'i_退款单量',
    refund_mct_amount                                DECIMAL(14, 2) COMMENT 'i_退款订单中商家结算金额',
    refund_amount                                    DECIMAL(14, 2) COMMENT 'i_退款金额'
) COMMENT '自营店铺退款率报表' PARTITIONED BY ( pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_finance_self_merchant_refund_detail/"
;



