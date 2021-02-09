insert overwrite table dwd.dwd_fd_app_checkin partition (pt='${pt}')
select
    /*+ REPARTITION(1) */
    project,
    platform_type,
    country_code,
    checkin_userid,
    checkin_userid_first,
    all_check_user_id,
    all_cont_check_user_id,
    acc_check_user_id_1th,
    acc_check_user_id_2th,
    acc_check_user_id_3th,
    acc_check_user_id_4th,
    acc_check_user_id_5th,
    acc_check_user_id_6th,
    acc_check_user_id_7th,
    acc_check_user_id_greater_7th,
    cont_check_user_id_2th,
    cont_check_user_id_3th,
    cont_check_user_id_4th,
    cont_check_user_id_5th,
    cont_check_user_id_6th,
    cont_check_user_id_7th,
    cont_check_user_id_greater_7th
from(
    select
        t2.project as project,
        t2.platform_type as platform_type,
        nvl(t4.region_code,t5.country_code) as country_code,
        null as checkin_userid,
        null as checkin_userid_first,
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
        if(t1.per_count >7 ,t1.user_id,null) as cont_check_user_id_greater_7th
    from (
        select
        t0.user_id,
        t0.count,
        t0.per_count,
        date(TO_UTC_TIMESTAMP(t0.last_date, 'America/Los_Angeles')) as last_date
        from ods_fd_vb.ods_fd_user_check_in t0
        where date(TO_UTC_TIMESTAMP(t0.last_date, 'America/Los_Angeles')) = '${pt}'
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
            where project is not null and project !=''
            and type in('android_ap','ios_app')
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

    ) t5 on t1.user_id = t5.user_id

    union all

    select
            t1.project as project,
            t1.platform_type as platform_type,
            t1.country_code as country_code,
            if(t1.check_date = '${pt}',t1.user_id,null) as checkin_userid,
            if(t1.check_date = '${pt}' and t1.rank = 1,t1.user_id,null) as checkin_userid_first,
            null as all_check_user_id,
            null as all_cont_check_user_id,
            null as acc_check_user_id_1th,
            null as acc_check_user_id_2th,
            null as acc_check_user_id_3th,
            null as acc_check_user_id_4th,
            null as acc_check_user_id_5th,
            null as acc_check_user_id_6th,
            null as acc_check_user_id_7th,
            null as acc_check_user_id_greater_7th,
            null as cont_check_user_id_2th,
            null as cont_check_user_id_3th,
            null as cont_check_user_id_4th,
            null as cont_check_user_id_5th,
            null as cont_check_user_id_6th,
            null as cont_check_user_id_7th,
            null as cont_check_user_id_greater_7th
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
        where ul.project is not null and ul.project !='' and r.region_code is not null and r.region_code != ''
    )t1
)checkin_table
;