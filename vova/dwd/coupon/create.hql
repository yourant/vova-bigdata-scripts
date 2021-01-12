drop table dwd.dwd_vova_fact_coupon;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd.dwd_vova_fact_coupon
(
    datasource          string comment '数据平台',
    cpn_id     bigint COMMENT '优惠券ID',
    cpn_code   string COMMENT '优惠券CODE',
    buyer_id   bigint COMMENT '领取用户ID',
    order_id   bigint COMMENT '使用优惠券的order_id',
    give_time  timestamp COMMENT '用户领取优惠券时间',
    used_time  timestamp COMMENT '优惠券使用时间',
    used_times bigint COMMENT '此红包已经使用的次数',
    cpn_status bigint COMMENT '优惠券状态'
) COMMENT '优惠券领用事实表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

