DROP TABLE dwb.dwb_vova_finance_bonus_card;
CREATE TABLE IF NOT EXISTS dwb.dwb_vova_finance_bonus_card
(
    datasource            string COMMENT '',
    interval_date         string COMMENT '',
    life_cycle            string COMMENT '',
    bonus_card_id         bigint COMMENT '',
    user_id               bigint COMMENT '',
    price                 decimal(10, 2) COMMENT '月卡费用',
    bonus_start           timestamp COMMENT '',
    bonus_end             timestamp COMMENT '',
    currency              string COMMENT '本月发放优惠券币种',
    issue_amount          decimal(15, 2) COMMENT '本月-发放优惠券金额',
    bonus                 decimal(15, 2) COMMENT '本月-抵扣优惠券金额USD',
    valid_amount          decimal(15, 2) COMMENT '本月-发放且未失效优惠券金额',
    order_cnt             bigint COMMENT '本月-优惠券支付订单量',
    order_amount          decimal(15, 2) COMMENT '本月-优惠券支付订单金额',
    issue_amount_interval decimal(15, 2) COMMENT '月卡周期-发放优惠券金额',
    bonus_interval        decimal(15, 2) COMMENT '月卡周期-抵扣优惠券金额USD',
    valid_amount_interval decimal(15, 2) COMMENT '月卡周期-发放且未失效优惠券金额',
    order_cnt_interval    bigint COMMENT '月卡周期-优惠券支付订单量',
    order_amount_interval decimal(15, 2) COMMENT '月卡周期-优惠券支付订单金额',
    income                decimal(15, 2) COMMENT '开卡费用'
) COMMENT '财务月卡报表'
    PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

