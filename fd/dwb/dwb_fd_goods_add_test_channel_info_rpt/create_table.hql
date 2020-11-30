CREATE TABLE IF NOT EXISTS `dwb.dwb_fd_goods_add_test_channel_info`(
  `project_name` string COMMENT '组织',
  `platform` string COMMENT '平台',
  `country` string COMMENT '国家',
  `cat_name` string COMMENT '品类',
  `ga_channel` string COMMENT '投放渠道',
  `add_uv` string COMMENT '加车 session数',
  `view_uv` string COMMENT 'view session数',
  `order_id` bigint COMMENT '订单数',
  `goods_amount` DECIMAL(15, 4) COMMENT '订单金额',
  `goods_test_goods_id` bigint COMMENT '测款商品数',
  `success_goods_test_goods_id` bigint COMMENT '测款成功商品数',
  `success_order_id` bigint COMMENT '测款成功的订单数',
  `success_goods_amount` DECIMAL(15, 4) COMMENT '测款成功的订单金额')
COMMENT '商品加购测款渠道信息表'
PARTITIONED BY (
  `pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");