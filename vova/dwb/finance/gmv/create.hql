DROP TABLE dwb.dwb_vova_finance_gmv;
CREATE TABLE IF NOT EXISTS dwb.dwb_vova_finance_gmv
(
    event_date                                         string COMMENT 'd_日期',
    mct_name                                           string COMMENT 'd_商家',
    source                                             string COMMENT 'd_数据来源',
    payment_name                                       string COMMENT 'd_支付方式',
    total_shop_price_amount                            DECIMAL(14, 2) COMMENT 'i_商品收入',
    total_shipping_fee                                 DECIMAL(14, 2) COMMENT 'i_运费收入',
    total_bonus                                        DECIMAL(14, 2) COMMENT 'i_红包',
    total_receive_amount                               DECIMAL(14, 2) COMMENT 'i_总收款金额',
    merchant_duty_refund_amount                        DECIMAL(14, 2) COMMENT 'i_扣佣金退款',
    non_merchant_duty_refund_amount                    DECIMAL(14, 2) COMMENT 'i_不扣佣金退款',
    total_refund_amount                                DECIMAL(14, 2) COMMENT 'i_总退款金额（财务实际退款金额）',
    total_refund_bonus                                 DECIMAL(14, 2) COMMENT 'i_退款红包',
    total_actual_refund_amount                         DECIMAL(14, 2) COMMENT 'i_应退款金额',
    brand_value_added                                  DECIMAL(14, 2) COMMENT 'i_收款中包含的品牌增值',
    refund_brand_value_added                           DECIMAL(14, 2) COMMENT 'i_退款中包含的品牌增值',
    total_container_transportation_shipping_fee        DECIMAL(14, 2) COMMENT 'i_收款中包含的集运增值',
    total_refund_container_transportation_shipping_fee DECIMAL(14, 2) COMMENT 'i_退款中包含的集运增值',
    auction_value_added                                DECIMAL(14, 2) COMMENT 'i_收款中包含的拍卖增值',
    refund_auction_value_added                         DECIMAL(14, 2) COMMENT 'i_退款中包含的拍卖增值',
    lucky_order_amount                                 DECIMAL(14, 2) COMMENT 'i_夺宝收款金额',
    lucky_bonus                                        DECIMAL(14, 2) COMMENT 'i_夺宝红包'
) COMMENT '财务gmv报表'
    PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

