CREATE TABLE IF NOT EXISTS `dwb.dwb_fd_rpt_main_process_rpt`(
  `project` string COMMENT '网站名称',
  `platform_type` string COMMENT '平台类型',
  `country` string COMMENT '国家',
  `is_new_user` string COMMENT '是否新会话',
  `ga_channel` string COMMENT 'session来源渠道',
  `all_sessions` bigint COMMENT '用来计算总会话',
  `product_view_sessions` bigint COMMENT '用来计算详情页总会话',
  `add_sessions` bigint COMMENT '用来计算加车总会话',
  `checkout_sessions` bigint COMMENT '用来计算checkout总会话',
  `checkout_option_sessions` bigint COMMENT '用来计算下单总会话',
  `purchase_sessions` bigint COMMENT '用来计算完成订单总会话',
  `orders` bigint COMMENT '订单号',
  `goods_amount` decimal(15,4) COMMENT '订单商品金额',
  `bonus` decimal(15,4) COMMENT '订单折扣',
  `shipping_fee` decimal(15,4) COMMENT '订单运费',
  `pv_session` bigint COMMENT '用来计算用户访问一次的session_id'
 )
COMMENT '主流程数据表'
PARTITIONED BY (
  `pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");