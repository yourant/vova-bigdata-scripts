CREATE TABLE `dwb.dwb_fd_income_cost`(
  `party_name` string COMMENT '网站名称',
  `country_code` string COMMENT '国家代码',
  `country_name` string COMMENT '国家名字',
  `dimension_type` string COMMENT '计算维度',
  `dt` string COMMENT '日期',
  `purchase_cost` decimal(16,6) COMMENT '购买花费',
  `sale_amount` decimal(16,6) COMMENT '销售金额',
  `coupon_cost` decimal(16,6) COMMENT '优惠券金额',
  `ads_cost` decimal(16,6) COMMENT '广告金额',
  `refund_cost` decimal(16,6) COMMENT '退款金额',
  `total_cost` decimal(16,6) COMMENT '总花费')
COMMENT '国家损益表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC;