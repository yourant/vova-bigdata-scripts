CREATE EXTERNAL TABLE IF NOT EXISTS `dwd.dwd_fd_app_checkin`(
  `project` string COMMENT '组织',
  `platform_type` string COMMENT '平台',
  `country_code` string COMMENT '国家',
  `checkin_userid` string COMMENT '当天完成签到的用户user_id',
  `checkin_userid_first` string COMMENT '历史第一次完成签到的用户user_id',
  `all_check_user_id` string COMMENT '总的签到用户user_id',
  `all_cont_check_user_id` string COMMENT '连续签到用户user_id',
  `acc_check_user_id_1th` string COMMENT '累计签到1次用户user_id',
  `acc_check_user_id_2th` string COMMENT '累计签到2次用户user_id',
  `acc_check_user_id_3th` string COMMENT '累计签到3次用户user_id',
  `acc_check_user_id_4th` string COMMENT '累计签到4次用户user_id',
  `acc_check_user_id_5th` string COMMENT '累计签到5次用户user_id',
  `acc_check_user_id_6th` string COMMENT '累计签到6次用户user_id',
  `acc_check_user_id_7th` string COMMENT '累计签到7次用户user_id',
  `acc_check_user_id_greater_7th` string COMMENT '累计签到大于7次用户user_id',
  `cont_check_user_id_2th` string COMMENT '连续签到2次用户user_id',
  `cont_check_user_id_3th` string COMMENT '连续签到3次用户user_id',
  `cont_check_user_id_4th` string COMMENT '连续签到4次用户user_id',
  `cont_check_user_id_5th` string COMMENT '连续签到5次用户user_id',
  `cont_check_user_id_6th` string COMMENT '连续签到6次用户user_id',
  `cont_check_user_id_7th` string COMMENT '连续签到7次用户user_id',
  `cont_check_user_id_greater_7th` string COMMENT '连续签到大于7次用户user_id'
 )
COMMENT '用户签到表'
PARTITIONED BY (
  `pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;