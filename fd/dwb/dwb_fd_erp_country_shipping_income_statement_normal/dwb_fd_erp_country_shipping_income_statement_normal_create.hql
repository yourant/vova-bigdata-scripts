CREATE TABLE IF NOT EXISTS `dwb.dwb_fd_erp_country_income_statement_normal`(
  order_id bigint,
  party_name string comment '网站组织名称',
  country_code string comment '国家代码',
  country_name string comment '国家英文名',
  sales_amount decimal(15, 4) comment '销售额美元',
  coupon_cost decimal(15, 4) comment '红包花费美元',
  ads_cost decimal(15, 4) comment '广告花费美元'
)
COMMENT '网站国家日常损益发货维度不变数据(销售额、红包花费、广告花费)表'
PARTITIONED BY (
  `pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

