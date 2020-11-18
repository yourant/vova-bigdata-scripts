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
  `pt` string,
  `classify` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

set mapred.reduce.tasks=1;
insert overwrite table dwb.dwb_fd_app_retention_activity_report partition (dt='${hiveconf:dt}',classify='register')
select
    t1.project as project,
    t1.platform_type as platform_type,
    t1.country_code as country_code,
    null,
    t1.session_id as all_session,
    null,null,null,null,null,null,null,null,
    null,null,null,
    null,null,null,null,
    if(t1.user_id != '0' and t1.user_id is not null,t1.domain_userid,null) as user_login_domain_userid,
    null as user_register_domain_userid,
    null as user_new_domain_userid,
    null as user_new_register_domain_userid,
    null as user_order_id,
    null as user_new_order_id,
    null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
    null,null,null,null,null,null,

    null as user_new_first_order_id,
    null as user_new_first_coupon_order_id,
    null as user_new_first_success_order_id,
    null as user_new_first_success_coupon_order_id
from (
    select user_id,session_id,domain_userid,project,country as country_code,platform_type
    from ods.ods_fd_snowplow_all_event
    where  dt = '${hiveconf:dt}' and platform_type in ('android_app','ios_app') and project is not null and project != ''
) t1

union
select
	t1.project as project,
	t1.platform_type as platform_type,
	t1.country_code as country_code,
	null,
	null as all_session,
	null,null,null,null,null,null,null,null,
	null,null,null,
	null,null,null,null,
	null as user_login_domain_userid,
    if(t2.user_id is not null,t1.device_id,null) as user_register_domain_userid,
    null as user_new_domain_userid,
    null as user_new_register_domain_userid,
    null as user_order_id,
    null as user_new_order_id,
    null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
    null,null,null,null,null,null,

    null as user_new_first_order_id,
	null as user_new_first_coupon_order_id,
	null as user_new_first_success_order_id,
	null as user_new_first_success_coupon_order_id
from(
	select t0.user_id,t0.device_id,t0.platform_type,t0.project,t0.country_code
	 from (
         SELECT
            user_id,
            device_id,
            case
              when platform = 'android' then 'android_app'
              when platform = 'ios' then 'ios_app' end as platform_type,
            project_name as project,
            country_code,
            Row_Number() OVER (partition by user_id ORDER BY event_time asc) rank
         FROM ods_fd_vb.ods_fd_app_install_record
         where user_id !=0 and user_id is not null
         and project_name is not null and project_name !=''
         and platform !=''
     )t0 where t0.rank = 1

) t1 left join (
  select user_id from ods_fd_vb.ods_fd_users
  where date(TO_UTC_TIMESTAMP(reg_time, 'America/Los_Angeles')) = '${hiveconf:dt}'
) t2 on t1.user_id = t2.user_id

union
select
	t1.project as project,
	t1.platform_type as platform_type,
	t1.country_code as country_code,
	null,
	null as all_session,
	null,null,null,null,null,null,null,null,
	null,null,null,
	null,null,null,null,
	null as user_login_domain_userid,
	null as user_register_domain_userid,
    t1.device_id as user_new_domain_userid,
    null as user_new_register_domain_userid,
    null as user_order_id,
    null as user_new_order_id,
    null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
    null,null,null,null,null,null,

    null as user_new_first_order_id,
	null as user_new_first_coupon_order_id,
	null as user_new_first_success_order_id,
	null as user_new_first_success_coupon_order_id
from (
   select
      device_id,
      case
         when platform = 'android' then 'android_app'
         when platform = 'ios' then 'ios_app' end as platform_type,
      project_name as project,
      country_code
   from ods_fd_vb.ods_fd_app_install_record
   where platform !=''
   and project_name is not null and project_name !=''
   and date(TO_UTC_TIMESTAMP(event_time,'America/Los_Angeles')) = '${hiveconf:dt}'
)t1

union
select
	t1.project as project,
	t1.platform_type as platform_type,
	t1.country_code as country_code,
	null,
	null as all_session,
	null,null,null,null,null,null,null,null,
	null,null,null,
	null,null,null,null,
	null as user_login_domain_userid,
	null as user_register_domain_userid,
    null as user_new_domain_userid,
    if(t2.user_id is not null,t1.device_id,null) as user_new_register_domain_userid,
    null as user_order_id,
    null as user_new_order_id,
    null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
    null,null,null,null,null,null,

    null as user_new_first_order_id,
	null as user_new_first_coupon_order_id,
	null as user_new_first_success_order_id,
	null as user_new_first_success_coupon_order_id
from (
	select t0.user_id,t0.device_id,t0.project,t0.platform_type,t0.country_code
    	from (
	       select
	          user_id,
	          device_id,
	          case
	             when platform = 'android' then 'android_app'
	             when platform = 'ios' then 'ios_app' end as platform_type,
	          project_name as project,
	          country_code,
	          Row_Number() OVER (partition by user_id ORDER BY event_time asc) rank
	       from ods_fd_vb.ods_fd_app_install_record
	       where platform !=''
	       and project_name is not null and project_name !=''
	       and user_id !=0 and user_id is not null
	       and date(TO_UTC_TIMESTAMP(event_time, 'America/Los_Angeles')) = '${hiveconf:dt}'
    	) t0 where t0.rank =1
)t1
left join (
     select user_id from ods_fd_vb.ods_fd_users where date(TO_UTC_TIMESTAMP(reg_time, 'America/Los_Angeles')) = '${hiveconf:dt}'
) t2 on t1.user_id = t2.user_id

union
select
	t1.project_name as project,
	t2.platform_type as platform_type,
	t3.region_code as country_code,
	null,
	null as all_session,
	null,null,null,null,null,null,null,null,
	null,null,null,
	null,null,null,null,
	null as user_login_domain_userid,
	null as user_register_domain_userid,
	null as user_new_domain_userid,
	null as user_new_register_domain_userid,
	t1.order_id as user_order_id,
	null as user_new_order_id,
	null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
	null,null,null,null,null,null,

	null as user_new_first_order_id,
	null as user_new_first_coupon_order_id,
	null as user_new_first_success_order_id,
	null as user_new_first_success_coupon_order_id
from (
  select user_id,order_id,country,user_agent_id,project_name
    from ods_fd_vb.ods_fd_order_info
   where date(from_unixtime(order_time,'yyyy-MM-dd hh:mm:ss')) = '${hiveconf:dt}'
   and project_name is not null and project_name !=''
)t1
inner join (
		select
			user_agent_id,
   		case
            when is_app = 1 and os_type = 'ios' then 'ios_app'
            when is_app = 1 and os_type = 'android' then 'android_app' end as platform_type
		from dim.dim_fd_user_agent
		where is_app = 1
	) t2 on t2.user_agent_id = t1.user_agent_id
left join dim.dim_fd_region t3 on t3.region_id = t1.country

union
select
	t3.project_name as project,
	t4.platform_type as platform_type,
	t5.region_code as country_code,
	null,
	null as all_session,
	null,null,null,null,null,null,null,null,
	null,null,null,
	null,null,null,null,
	null as user_login_domain_userid,
	null as user_register_domain_userid,
	null as user_new_domain_userid,
	null as user_new_register_domain_userid,
	null as user_order_id,
	t3.order_id as user_new_order_id,
	null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
	null,null,null,null,null,null,

    t3.user_id as user_new_first_order_id,
	if(t3.coupon_code is not null and t3.coupon_code != '',t3.user_id,null) as user_new_first_coupon_order_id,
	if(t3.pay_status = 2,t3.user_id,null) as user_new_first_success_order_id,
	if(t3.coupon_code is not null and t3.coupon_code != '' and t3.pay_status = 2,t3.user_id,null) as user_new_first_success_coupon_order_id

from(
  select t2.user_id
  from (
    select t0.user_id
      from (
      select
              user_id,
              Row_Number() OVER (partition by user_id ORDER BY event_time asc) rank
         from ods_fd_vb.ods_fd_app_install_record
        where platform !=''
          and user_id !=0 and user_id is not null
          and date(TO_UTC_TIMESTAMP(event_time, 'America/Los_Angeles')) = '${hiveconf:dt}'
       )t0 where t0.rank = 1
  ) t2
  inner join (
     select user_id
       from ods_fd_vb.ods_fd_users
      where date(TO_UTC_TIMESTAMP(reg_time, 'America/Los_Angeles')) = '${hiveconf:dt}'
  ) t3 on t2.user_id = t3.user_id

) t2
left join (
  select user_id,order_id,country,user_agent_id,project_name,coupon_code,pay_status
    from ods_fd_vb.ods_fd_order_info
   where date(from_unixtime(order_time,'yyyy-MM-dd hh:mm:ss')) = '${hiveconf:dt}'
   and project_name !='' and project_name is not null
)t3 on t2.user_id = t3.user_id
inner join (
  select
    user_agent_id,
    case
      when is_app = 1 and os_type = 'ios' then 'ios_app'
      when is_app = 1 and os_type = 'android' then 'android_app' end as platform_type
  from dim.dim_fd_user_agent
  where is_app = 1
) t4 on t4.user_agent_id = t3.user_agent_id
left join dim.dim_fd_region t5 on t5.region_id = t3.country;
