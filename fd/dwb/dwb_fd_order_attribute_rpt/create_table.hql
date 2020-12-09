CREATE TABLE IF NOT EXISTS dwd.dwd_fd_snowplow_click_impr (
`project_name` string COMMENT '组织',
`country` string COMMENT '国家',
`platform_type` string COMMENT '平台',
`page_code` string COMMENT 'page_code',
`list_type` string COMMENT 'list_type',
`domain_userid` string COMMENT '设备id',
`event_name` string COMMENT '只有(goods click和impression事件)',
`session_id` string COMMENT 'session_id',
`derived_tstamp` bigint COMMENT '时间'
) COMMENT '打点数据中的goods click事件和impression事件'
PARTITIONED BY (`pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS dwd.dwd_fd_snowplow_add (
`project_name` string COMMENT '组织',
`country` string COMMENT '国家',
`platform_type` string COMMENT '平台',
`event_name` string COMMENT '只有add事件',
`domain_userid` string COMMENT '设备id',
`page_code` string COMMENT 'page_code',
`list_type` string COMMENT 'list_type'
) COMMENT '订单归因所需打点add事件表'
PARTITIONED BY (`pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS dwd.dwd_fd_snowplow_order (
`project_name` string COMMENT '组织',
`country` string COMMENT '国家',
`platform_type` string COMMENT '平台',
`page_code` string COMMENT 'page_code',
`list_type` string COMMENT 'list_type',
`user_id` string COMMENT '订单 用户id',
`domain_userid` string COMMENT '打点 设备id',
`order_id` string COMMENT '订单id',
`pay_status` string COMMENT '支付状态',
`order_amount` decimal(15,4) COMMENT '订单金额包含运费'
) COMMENT '打点数据和订单数据相关'
PARTITIONED BY (`pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS dwb.dwb_fd_order_attribution_rpt (
`project_name` string COMMENT '组织',
`country` string COMMENT '国家',
`platform_type` string COMMENT '平台',
`page_code` string COMMENT 'page_code',
`list_type` string COMMENT 'list_type',
`goods_impression_cnt` BIGINT COMMENT '曝光数',
`goods_click_cnt` bigint COMMENT '点击数',
`goods_impression_uv_cnt` bigint COMMENT '曝光UV',
`goods_click_uv_cnt` bigint COMMENT '点击UV',
`goods_add_uv_cnt` bigint COMMENT '加购成功UV',
`total_order_cnt` bigint COMMENT '订单数量',
`total_success_order_cnt` bigint COMMENT '支付成功订单数',
`total_order_user_uv_cnt` bigint COMMENT '支付成功订单的用户',
`gmv` decimal(15,4) COMMENT 'gmv'
) COMMENT '订单归因指标数据'
PARTITIONED BY (`pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

