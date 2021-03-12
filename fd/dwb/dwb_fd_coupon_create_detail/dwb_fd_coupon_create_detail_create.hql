CREATE TABLE IF NOT EXISTS dwb.dwb_fd_coupon_create_detail
(
	project_name     			string comment '组织',
    coupon_type_name            string comment '红包类型名',
  	coupon_create_cnt			bigint comment '当天创建量',
	coupon_gived_cnt			bigint comment '红包发放总量',
	coupon_use_fail_cnt         bigint comment '已使用未付款量',
	coupon_use_success_cnt      bigint comment '已使用已付款量',
	coupon_used_total_cnt		bigint comment '使用总量'
) comment '红包创建使用情况报表'
partitioned by (`pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
stored as parquet;