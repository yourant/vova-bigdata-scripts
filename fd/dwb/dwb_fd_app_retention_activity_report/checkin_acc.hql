CREATE TABLE IF NOT EXISTS `dwb.dwb_fd_app_retention_activity_report`(
  `project` string COMMENT '组织',
  `platform_type` string COMMENT '平台',
  `country_code` string COMMENT '国家',
  `all_domain_userid` string COMMENT '当天活跃的所有设备id',
  `all_session` string COMMENT '当天所有session数',
  `retention_domain_userid_2d` string COMMENT '前一天留存的设备id',
  `retention_domain_userid_1d` string COMMENT '当天和前一天都留存的设备id',
  `login_domain_userid_2d` string COMMENT '前一天登录用户的设备id',
  `login_domain_userid_1d` string COMMENT '当天和前一天都登录用户的设备id',
  `checkin_domain_userid_2d` string COMMENT '前一天签到用户的设备id',
  `checkin_domain_userid_1d` string COMMENT '当天和前天都签到用户的设备id',
  `play_domain_userid_2d` string COMMENT '前一天玩大转盘用户的设备id',
  `play_domain_userid_1d` string COMMENT '当天和前天都玩大转盘用户的设备id',
  `checkin_points_domain_userid` string COMMENT '当天访问积分页的设备id',
  `checkin_userid` string COMMENT '当天完成签到的用户user_id',
  `checkin_userid_first` string COMMENT '历史第一次完成签到的用户user_id',
  `play_visit_domain_userid` string COMMENT '当天访问大转盘的设备id',
  `play_join_userid` string COMMENT '当天参与大转盘的用户user_id',
  `play_first_join_userid` string COMMENT '历史首次参与大转盘的用户user_id',
  `play_points_join_userid` string COMMENT '积分参与大转盘的用户user_id',
  `user_login_domain_userid` string COMMENT '当天登录用户的设备id',
  `user_register_domain_userid` string COMMENT '当天注册用户的设备id',
  `user_new_domain_userid` string COMMENT '当天新用户的设备id',
  `user_new_register_domain_userid` string COMMENT '当天注册新用户用的设备id',
  `user_order_id` string COMMENT '当天下单的订单id，不限支付成功与否',
  `user_new_order_id` string COMMENT '当天新用户下单的订单id，不限支付成功与否',
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
  `cont_check_user_id_greater_7th` string COMMENT '连续签到大于7次用户user_id',
  `points_domain_userid` string COMMENT '访问积分页设备数(计算结果不去重)',
  `points_homepage_domain_userid` string COMMENT '首页入口进入积分页设备数(计算结果不去重)',
  `points_userzone_domain_userid` string COMMENT '新人专区进入积分页设备数(计算结果不去重)',
  `points_account_domain_userid` string COMMENT '个人中心进入积分页设备数(计算结果不去重)',
  `points_afterpay_domain_userid` string COMMENT '支付成功页进入积分页设备数(计算结果不去重)',
  `points_others_domain_userid` string COMMENT '其它页进入积分页设备数(计算结果不去重)',
  `user_new_first_order_id` string COMMENT '新用户生成首单总数',
  `user_new_first_coupon_order_id` string COMMENT '新用户使用coupon首单总数',
  `user_new_first_success_order_id` string COMMENT '新用户支付成功首单总数',
  `user_new_first_success_coupon_order_id` string COMMENT '新用户使用coupon支付成功首单总数')
COMMENT '用户留存，签到，大转盘和用户注册相关数据，数据来源业务表以及打点数据'
PARTITIONED BY (
  `dt` string,
  `classify` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");


set mapred.reduce.tasks=1;
insert overwrite table dwb.dwb_fd_app_retention_activity_report partition (dt='${hiveconf:dt}',classify='checkin_acc')
select
    t2.project as project,
    t2.platform_type as platform_type,
    nvl(t4.region_code,t5.country_code) as country_code,
    null,null,
    null,null,null,null,null,null,null,null,
    null,null,null,
    null,null,null,null,
    null,null,null,null,null,null,
    t1.user_id as all_check_user_id,
    if(t1.per_count > 1,t1.user_id,null) as all_cont_check_user_id,
    if(t1.count = 1,t1.user_id,null) as acc_check_user_id_1th,
    if(t1.count = 2,t1.user_id,null) as acc_check_user_id_2th,
    if(t1.count = 3,t1.user_id,null) as acc_check_user_id_3th,
    if(t1.count = 4,t1.user_id,null) as acc_check_user_id_4th,
    if(t1.count = 5,t1.user_id,null) as acc_check_user_id_5th,
    if(t1.count = 6,t1.user_id,null) as acc_check_user_id_6th,
    if(t1.count = 7,t1.user_id,null) as acc_check_user_id_7th,
    if(t1.count > 7,t1.user_id,null) as acc_check_user_id_greater_7th,
    if(t1.per_count =2 ,t1.user_id,null) as cont_check_user_id_2th,
    if(t1.per_count =3 ,t1.user_id,null) as cont_check_user_id_3th,
    if(t1.per_count =4 ,t1.user_id,null) as cont_check_user_id_4th,
    if(t1.per_count =5 ,t1.user_id,null) as cont_check_user_id_5th,
    if(t1.per_count =6 ,t1.user_id,null) as cont_check_user_id_6th,
    if(t1.per_count =7 ,t1.user_id,null) as cont_check_user_id_7th,
    if(t1.per_count >7 ,t1.user_id,null) as cont_check_user_id_greater_7th,
    null,null,null,null,null,null,
    null,null,null,null
from (
  select
    t0.user_id,
    t0.count,
    t0.per_count,
    date(TO_UTC_TIMESTAMP(t0.last_date, 'America/Los_Angeles')) as last_date
  from ods_fd_vb.ods_fd_user_check_in t0
  where date(TO_UTC_TIMESTAMP(t0.last_date, 'America/Los_Angeles')) = '${hiveconf:dt}'
) t1
left join(
  select t0.user_id,t0.project,t0.platform_type
  from (
    select
      user_id,
      project,
      case
        when type = 'android_ap' then 'android_app'
        when type = 'ios_app' then 'ios_app' end as platform_type,
      Row_Number() OVER (partition by user_id ORDER BY time desc) rank
    from ods_fd_vb.ods_fd_user_check_in_log
  ) t0 where t0.rank = 1

) t2 on t1.user_id = t2.user_id
left join ods_fd_vb.ods_fd_users t3 on t3.user_id = t1.user_id
left join dim.dim_fd_region t4 on t4.region_id = t3.country
left join (
  select t0.user_id,t0.country_code
  from (
    select user_id,country_code,Row_Number() OVER (partition by user_id ORDER BY event_time desc) rank
    from ods_fd_vb.ods_fd_app_install_record where user_id is not null
  ) t0 where t0.rank = 1

) t5 on t1.user_id = t5.user_id;
