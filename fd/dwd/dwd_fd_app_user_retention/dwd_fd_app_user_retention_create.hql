<<<<<<< Updated upstream
CREATE EXTERNAL TABLE `dwd.dwd_fd_app_user_retention`(
  `project` string COMMENT '组织',
  `platform_type` string COMMENT '平台',
  `country_code` string COMMENT '国家',
  `retention_domain_userid_2d` string COMMENT '前一天留存的设备id',
  `retention_domain_userid_1d` string COMMENT '当天和前一天都留存的设备id',
  `login_domain_userid_2d` string COMMENT '前一天登录用户的设备id',
  `login_domain_userid_1d` string COMMENT '当天和前一天都登录用户的设备id',
  `checkin_domain_userid_2d` string COMMENT '前一天签到用户的设备id',
  `checkin_domain_userid_1d` string COMMENT '当天和前天都签到用户的设备id',
  `play_domain_userid_2d` string COMMENT '前一天玩大转盘用户的设备id',
  `play_domain_userid_1d` string COMMENT '当天和前天都玩大转盘用户的设备id')
=======
CREATE EXTERNAL TABLE IF NOT EXISTS `dwd.dwd_fd_app_user_retention`(
    `project` string COMMENT '组织',
    `platform_type` string COMMENT '平台',
    `country_code` string COMMENT '国家',
    `retention_domain_userid_2d` string COMMENT '前一天留存的设备id',
    `retention_domain_userid_1d` string COMMENT '当天和前一天都留存的设备id',
    `login_domain_userid_2d` string COMMENT '前一天登录用户的设备id',
    `login_domain_userid_1d` string COMMENT '当天和前一天都登录用户的设备id',
    `checkin_domain_userid_2d` string COMMENT '前一天签到用户的设备id',
    `checkin_domain_userid_1d` string COMMENT '当天和前天都签到用户的设备id',
    `play_domain_userid_2d` string COMMENT '前一天玩大转盘用户的设备id',
    `play_domain_userid_1d` string COMMENT '当天和前天都玩大转盘用户的设备id'
 )
>>>>>>> Stashed changes
COMMENT '用户留存'
PARTITIONED BY (
  `pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;