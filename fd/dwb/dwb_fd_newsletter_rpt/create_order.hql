CREATE TABLE IF NOT EXISTS dwb.dwb_fd_newsletter_order_rpt (
`project` string COMMENT '组织',
`order_date_utc` string COMMENT '下单时间',
`order_id` string COMMENT '订单id',
`order_sn` string COMMENT '订单sn',
`country_name` string COMMENT '国家名',
`country_code` string COMMENT '国家code',
`platform_type` string COMMENT '平台',
`nl_code` string COMMENT 'nl_code',
`goods_id` string COMMENT '商品id',
`virtual_goods_id` string COMMENT '商品虚拟id',
`cat_name` string COMMENT '商品类别名',
`goods_number` bigint COMMENT '商品销售量',
`shop_price`  decimal(15,4) COMMENT '商品销售总价'
) COMMENT 'Newsltter 订单报表'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");