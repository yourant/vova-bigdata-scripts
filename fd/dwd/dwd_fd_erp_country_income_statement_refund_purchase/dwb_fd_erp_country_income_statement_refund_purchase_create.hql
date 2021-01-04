CREATE TABLE IF NOT EXISTS `dwd.dwd_fd_erp_country_income_statement_refund_purchase`(
  data_type string,
  order_id bigint,
  purchase_cost decimal(15, 4) comment '采购花费美元',
  refund_cost decimal(15, 4) comment '退款花费美元'
)
COMMENT '网站国家日常损益变动数据(采购花费、退款花费)表'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;