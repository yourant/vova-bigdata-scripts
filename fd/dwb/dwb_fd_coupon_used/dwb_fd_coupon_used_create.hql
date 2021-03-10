CREATE TABLE IF NOT EXISTS dwb.dwb_fd_coupon_used
(
    project_name                string comment '组织',
    coupon_type_name            string comment '红包类型名',
    goods_amount_total          decimal(15,4) comment '商品金额',
    bonus_total                 decimal(15,4) comment '优惠金额',
    pt_goods_amount             decimal(15,4) comment 'pt-商品金额',
    pt_bonus                    decimal(15,4) comment 'pt-优惠金额'
) comment '红包使用情况报表'
partitioned by (`pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
stored as parquet;