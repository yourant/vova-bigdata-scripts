CREATE TABLE IF NOT EXISTS `dwd.dwd_fd_erp_country_income_statement_normal_paid`(
      order_id bigint,
      party_name string comment '网站组织名称',
      country_code string comment '国家代码',
      country_name string comment '国家英文名',
      sales_amount decimal(15, 4) comment '销售额美元',
      coupon_cost decimal(15, 4) comment '红包花费美元',
      ads_cost decimal(15, 4) comment '广告花费美元',
      purchase_cost decimal(15, 4) comment '预估采购花费美元'
)
COMMENT '网站国家日常损益下单维度已支付不变数据(销售额、红包花费、广告花费)表'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;