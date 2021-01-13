CREATE TABLE IF NOT EXISTS `ads.ads_fd_income_cost`(
  `project` string COMMENT '网站名称',
  `country_code` string COMMENT '国家代码',
  `country_name` string COMMENT '国家名字',
  `dimension_type` string COMMENT '计算维度',
  `pt_date` string COMMENT '日期',
  `purchase_cost` decimal(15,4) COMMENT '购买花费',
  `sale_amount` decimal(15,4) COMMENT '销售金额',
  `coupon_cost` decimal(14,4) COMMENT '优惠券金额',
  `ads_cost` decimal(15,4) COMMENT '广告金额',
  `refund_cost` decimal(15,4) COMMENT '退款金额',
  `total_cost` decimal(15,4) COMMENT '总花费'
)
COMMENT '国家损益表'
PARTITIONED BY (`pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;