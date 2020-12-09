CREATE TABLE IF NOT EXISTS dwb.dwb_fd_newsletter_snowplow_rpt (
`year` string COMMENT '年',
`month` string COMMENT '月',
`weekofyear` string COMMENT '一年中的第几周',
`project` string COMMENT '组织',
`nl_code_num` string COMMENT 'nl期数',
`nl_code` string COMMENT 'nl code',
`nl_type` string COMMENT 'nl type',
`nl_module` string COMMENT '模块',
`domain_userid` string COMMENT 'domain_userid',
`add_domain_userid` string COMMENT 'add_domain_userid',
`goods_click_domian_userid` string COMMENT 'goods_click_domian_userid',
`goods_impression_domain_userid` string COMMENT 'goods_impression_domain_userid',
`order_domain_userid` string COMMENT 'order_domain_userid',
`order_id` string COMMENT '订单id',
`goods_amount` decimal(15, 4) COMMENT '销售金额'
) COMMENT 'Newsltter 打点数据'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");