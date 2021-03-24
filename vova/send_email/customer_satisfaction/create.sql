drop table dwb.dwb_vova_customer_satisfaction;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_customer_satisfaction
(
  `order_sn` string,
  `order_goods_id` string,
  `email` string,
  `buyer_name` string,
  `tel` string,
  `region_code` string,
  `name_cn` string,
  `confirm_time` string,
  `shipping_status` string,
  `refund_type` string,
  `consignee` string COMMENT '收货人姓名',
  `is_re_buy` string
) COMMENT 'customer_satisfaction' PARTITIONED BY (pt STRING)
     STORED AS PARQUETFILE;