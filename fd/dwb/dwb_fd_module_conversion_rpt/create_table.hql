CREATE TABLE if not EXISTS `dwb.dwb_fd_module_conversion_rpt`(
    `module_name` string COMMENT '模块名',
    `platform` string COMMENT '平台',
    `project` string COMMENT '组织',
    `country` string COMMENT '国家',
    `cat_name` string COMMENT '品类',
    `impression_uv` bigint comment '曝光量',
    `click_uv` bigint COMMENT '点击量',
    `action_users` bigint COMMENT 'action_users',
    `order_num` bigint COMMENT '订单量',
    `goods_num` bigint COMMENT '销量',
    `goods_amount` decimal(10,2) COMMENT '销售额',
    `ctr` decimal(10,2) COMMENT 'ctr',
    `impression of action` string COMMENT '',
    `impression of orders` string COMMENT ''
)
COMMENT '模块转化报表'
PARTITIONED BY (
`pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS orc
TBLPROPERTIES ("orc.compress"="SNAPPY");


CREATE TABLE  if not exists `dwd.dwd_fd_common_module_interact`(
`project` string,
`domain_userid` string,
`session_id` string,
`event_name` string,
`event_step` string,
`platform_type` string,
`country` string,
`app_version` string,
`module_name` string,
`goods_id` string)
PARTITIONED BY (
  `pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS orc
TBLPROPERTIES ("orc.compress"="SNAPPY");