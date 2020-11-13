CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_users(
  `user_id` bigint COMMENT '下单时的用户ID',
  `userid` string COMMENT 'hash后用户id',
  `email` string COMMENT '用户邮箱',
  `user_name` string COMMENT '用户姓名',
  `password` string COMMENT '密码',
  `question` string COMMENT '密保问题',
  `answer` string COMMENT '答案',
  `gender` string COMMENT '性别',
  `birthday` string COMMENT '生日',
  `address_id` bigint COMMENT '地址',
  `reg_time` string COMMENT '注册时间',
  `last_time` string COMMENT '',
  `last_ip` string COMMENT '',
  `visit_count` int COMMENT '',
  `points` int COMMENT '',
  `user_rank` tinyint COMMENT '等级',
  `is_special` tinyint COMMENT '',
  `salt` string COMMENT '',
  `user_realname` string COMMENT '用户真实姓名',
  `user_mobile` string COMMENT '手机',
  `country` int COMMENT '国家',
  `province` int COMMENT '省',
  `city` int COMMENT '城市',
  `district` int COMMENT '地区',
  `zipcode` string COMMENT '邮编',
  `user_address` string COMMENT '联系地址',
  `track_id` string COMMENT '',
  `reg_source` bigint COMMENT '注册来源',
  `reg_province` bigint COMMENT '注册来源地',
  `reg_recommender` string COMMENT '注册推荐人',
  `user_tel` string COMMENT '用户联系方式',
  `email_valid` tinyint COMMENT '是否邮箱验证',
  `email_validate_point_id` bigint COMMENT '通过邮箱验证获得积分',
  `unsubscribe` tinyint COMMENT '是否取消订阅',
  `is_delete` tinyint COMMENT '是否删除',
  `language_id` int COMMENT '语言id',
  `reg_site_name` string COMMENT '注册网站',
  `reg_site_host` string COMMENT '注册域名',
  `reg_page` string COMMENT '注册页面',
  `open_id` string COMMENT '第三方平台id',
  `app_push_number` int COMMENT '推送'
  )COMMENT '用户信息表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_users
select `(dt)?+.+` from ods_fd_vb.ods_fd_users_arc 
where dt = '${hiveconf:dt}';
