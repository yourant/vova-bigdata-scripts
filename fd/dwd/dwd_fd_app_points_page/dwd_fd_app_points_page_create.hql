CREATE EXTERNAL TABLE IF NOT EXISTS `dwd.dwd_fd_app_points_page`(
    `project` string COMMENT '组织',
    `platform_type` string COMMENT '平台',
    `country_code` string COMMENT '国家',
    `all_domain_userid` string COMMENT '当天活跃的所有设备id',
    `all_session` string COMMENT '当天所有session数',
    `checkin_points_domain_userid` string COMMENT '当天访问积分页的设备id',
    `play_visit_domain_userid` string COMMENT '当天访问大转盘的设备id',
    `user_login_domain_userid` string COMMENT '当天登录用户的设备id',
    `points_domain_userid` string COMMENT '访问积分页设备数(计算结果不去重)',
    `points_homepage_domain_userid` string COMMENT '首页入口进入积分页设备数(计算结果不去重)',
    `points_userzone_domain_userid` string COMMENT '新人专区进入积分页设备数(计算结果不去重)',
    `points_account_domain_userid` string COMMENT '个人中心进入积分页设备数(计算结果不去重)',
    `points_afterpay_domain_userid` string COMMENT '支付成功页进入积分页设备数(计算结果不去重)',
    `points_others_domain_userid` string COMMENT '其它页进入积分页设备数(计算结果不去重)'
)
COMMENT '用户访问积分页'
PARTITIONED BY (
  `pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;