insert overwrite table dwd.dwd_fd_app_play_wheel partition (pt='${pt}')
select
    /*+ REPARTITION(1) */
    nvl(t2.project,t3.project) as project,
    nvl(t2.platform_type,t3.platform_type) as platform_type,
    nvl(t2.country_code,t3.country_code) as country_code,
    t1.user_id as play_join_userid,
    if(t1.rank = 1,t1.user_id,null) as play_first_join_userid,
    if(t1.rank > 1,t1.user_id,null) as play_points_join_userid
from (

      select
        user_id,
        device_id,
        date(TO_UTC_TIMESTAMP(winning_time, 'America/Los_Angeles')) as first_play_date,
        Row_Number() OVER (partition by user_id ORDER BY winning_time asc) rank
      from ods_fd_vb.ods_fd_turntable_record_v2
      where date(TO_UTC_TIMESTAMP(winning_time, 'America/Los_Angeles')) = '${pt}'
) t1
left join (
    select t0.user_id,t0.project,t0.country_code,t0.platform_type
    from (
        select
          user_id,
          project_name as project,
          country_code,
          case
          when platform = 'android' then 'android_app'
          when platform = 'ios' then 'ios_app' end as platform_type,
          Row_Number() OVER (partition by user_id ORDER BY event_time desc) rank
        from ods_fd_vb.ods_fd_app_install_record
    ) t0 where t0.rank = 1
) t2 on t1.user_id = t2.user_id
left join (
    select t0.device_id,t0.project,t0.country_code,t0.platform_type
    from (
      select
          device_id,
          project_name as project,
          country_code,
          case
          when platform = 'android' then 'android_app'
          when platform = 'ios' then 'ios_app' end as platform_type,
          Row_Number() OVER (partition by device_id ORDER BY event_time desc) rank
      from ods_fd_vb.ods_fd_app_install_record
    ) t0 where t0.rank = 1
) t3 on t1.device_id = t3.device_id;
