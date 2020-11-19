CREATE TABLE IF NOT EXISTS `dwb.dwb_fd_app_retention_activity_rpt`(
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
  `pt` string,
  `classify` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

set mapred.reduce.tasks=1;
insert overwrite table dwb.dwb_fd_app_retention_activity_rpt partition (pt='${hiveconf:pt}',classify='checkin')
select
      project as project,
      platform_type as platform_type,
      country as country_code,
      domain_userid as all_domain_userid,
      null,
      null,null,null,null,null,null,null,null,
      if(page_code = 'myrewards',domain_userid,null) as checkin_points_domain_userid,
      null as checkin_userid,
      null as checkin_userid_first,
      null,null,null,null,
      null,null,null,null,null,null,
      null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
      null,null,null,null,null,null,
	  null,null,null,null
from ods.ods_fd_snowplow_all_event
where pt = '${hiveconf:pt}' and platform_type in ('android_app','ios_app') and page_code = 'myrewards'
and project is not null and project !=''

union
select
	ul.project as project,
	case when ul.type = 'android_ap' then 'android_app'
		 when ul.type = 'ios_app' then 'ios_app' end as platform_type,
	r.region_code as country_code,
	null as all_domain_userid,
	null,
	null,null,null,null,null,null,null,null,
	null as checkin_points_domain_userid,
	ul.user_id as checkin_userid,
	null as checkin_userid_first,
	null,null,null,null,
	null,null,null,null,null,null,
	null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
	null,null,null,null,null,null,
	null,null,null,null
from ods_fd_vb.ods_fd_user_check_in_log ul
left join ods_fd_vb.ods_fd_users u on ul.user_id = u.user_id
left join dim.dim_fd_region r on u.country = r.region_id
where date(TO_UTC_TIMESTAMP(ul.time, 'America/Los_Angeles')) = '${hiveconf:pt}'
and ul.project is not null and ul.project !=''

union
select
	t1.project,
	t1.platform_type,
	t1.country_code,
	null as all_domain_userid,
	null,
	null,null,null,null,null,null,null,null,
	null as checkin_points_domain_userid,
	null as checkin_userid,
	t1.user_id as checkin_userid_first,
	null,null,null,null,
	null,null,null,null,null,null,
	null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
	null,null,null,null,null,null,
	null,null,null,null
from (
    select
		ul.user_id,
		ul.project,
		case when ul.type = 'android_ap' then 'android_app'
			 when ul.type = 'ios_app' then 'ios_app' end as platform_type,
		r.region_code as country_code,
		date(TO_UTC_TIMESTAMP(ul.time, 'America/Los_Angeles')) as check_date,
        Row_Number() OVER (partition by ul.user_id ORDER BY ul.time asc) rank
    from ods_fd_vb.ods_fd_user_check_in_log ul
    left join ods_fd_vb.ods_fd_users u on ul.user_id = u.user_id
    left join dim.dim_fd_region r on u.country = r.region_id
) t1 where t1.rank = 1 and t1.check_date = '${hiveconf:pt}';
