CREATE EXTERNAL TABLE IF NOT EXISTS `dwd.dwd_fd_app_play_wheel`(
  `project` string COMMENT '组织',
  `platform_type` string COMMENT '平台',
  `country_code` string COMMENT '国家',
  `play_join_userid` string COMMENT '当天参与大转盘的用户user_id',
  `play_first_join_userid` string COMMENT '历史首次参与大转盘的用户user_id',
  `play_points_join_userid` string COMMENT '积分参与大转盘的用户user_id'
)
COMMENT '用户参与大转盘'
PARTITIONED BY (
  `pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;