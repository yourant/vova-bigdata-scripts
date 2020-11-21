insert overwrite table dwb.dwb_fd_app_retention_activity partition (pt='${hiveconf:pt}',classify='checkin_acc')
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
  where date(TO_UTC_TIMESTAMP(t0.last_date, 'America/Los_Angeles')) = '${hiveconf:pt}'
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
