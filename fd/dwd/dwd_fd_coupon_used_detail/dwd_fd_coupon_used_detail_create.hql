CREATE TABLE IF NOT EXISTS dwd.dwd_fd_coupon_used_detail
(
    project_name                string comment '组织',
    coupon_code                 string comment '红包code',
    pay_time_prc                string comment '支付时间',
    pay_status                  bigint comment '支付状态',
    bonus                       decimal(15,4) comment '优惠金额',
    goods_amount                decimal(15,4) comment '商品金额',
    coupon_type_name            string comment '红包类型名'
) comment '红包使用情况报表'
partitioned by (`pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
stored as parquet;