insert overwrite table dwd.dwd_fd_app_register partition (pt='${pt}')
select /*+ REPARTITION(1) */
        project,
        platform_type,
        country_code,
        user_register_domain_userid,
        user_new_domain_userid,
        user_new_register_domain_userid,
        user_order_id,
        user_new_order_id,
        user_new_first_order_id,
        user_new_first_coupon_order_id,
        user_new_first_success_order_id,
        user_new_first_success_coupon_order_id
from(
        select
            t1.project as project,
            t1.platform_type as platform_type,
            t1.country_code as country_code,
            if(t2.user_id is not null and t1.rank = 1,t1.device_id,null) as user_register_domain_userid,
            if(t1.event_date = '${pt}',t1.device_id,null) as user_new_domain_userid,
            if(t2.user_id is not null and t1.rank = 1 and t1.event_date = t2.reg_date ,t1.device_id,null) as user_new_register_domain_userid,
            null as user_order_id,
            null as user_new_order_id,
            null as user_new_first_order_id,
            null as user_new_first_coupon_order_id,
            null as user_new_first_success_order_id,
            null as user_new_first_success_coupon_order_id
        from(

             SELECT
                user_id,
                device_id,
                case
                  when platform = 'android' then 'android_app'
                  when platform = 'ios' then 'ios_app' end as platform_type,
                project_name as project,
                country_code,
                date(TO_UTC_TIMESTAMP(event_time,'America/Los_Angeles')) as event_date,
                Row_Number() OVER (partition by user_id ORDER BY event_time asc) rank
             FROM ods_fd_vb.ods_fd_app_install_record
             where user_id !=0 and user_id is not null
             and project_name is not null and project_name !=''
             and platform !=''

        ) t1 left join (
            select user_id,date(TO_UTC_TIMESTAMP(reg_time, 'America/Los_Angeles')) as reg_date
            from ods_fd_vb.ods_fd_users
            where date(TO_UTC_TIMESTAMP(reg_time, 'America/Los_Angeles')) = '${pt}'
        ) t2 on t1.user_id = t2.user_id

        union all
        select
            t3.project_name as project,
            t4.platform_type as platform_type,
            t5.region_code as country_code,
            null as user_register_domain_userid,
            null as user_new_domain_userid,
            null as user_new_register_domain_userid,
            t3.order_id as user_order_id,
            if(t2.user_id is not null,t3.order_id,null) as user_new_order_id,
            if(t2.user_id is not null,t3.user_id,null) as user_new_first_order_id,
            if(t2.user_id is not null and t3.coupon_code is not null and t3.coupon_code != '',t3.user_id,null) as user_new_first_coupon_order_id,
            if(t2.user_id is not null and t3.pay_status = 2,t3.user_id,null) as user_new_first_success_order_id,
            if(t2.user_id is not null and t3.coupon_code is not null and t3.coupon_code != '' and t3.pay_status = 2,t3.user_id,null) as user_new_first_success_coupon_order_id

        from(
            select user_id,order_id,country,user_agent_id,project_name,coupon_code,pay_status
            from ods_fd_vb.ods_fd_order_info
            where date(to_utc_timestamp(order_time, 'America/Los_Angeles')) = '${pt}'
            and project_name !='' and project_name is not null

        ) t3
        left join (

            select tab2.user_id
            from (
                select t0.user_id
                from (
                    select
                          user_id,
                          Row_Number() OVER (partition by user_id ORDER BY event_time asc) rank
                     from ods_fd_vb.ods_fd_app_install_record
                    where platform !=''
                      and user_id !=0 and user_id is not null
                      and date(TO_UTC_TIMESTAMP(event_time, 'America/Los_Angeles')) = '${pt}'
                )t0 where t0.rank = 1
            ) tab2
            inner join (
                select user_id
                from ods_fd_vb.ods_fd_users
                where date(TO_UTC_TIMESTAMP(reg_time, 'America/Los_Angeles')) = '${pt}'
            ) tab3 on tab2.user_id = tab3.user_id

        )t2 on t2.user_id = t3.user_id

        inner join (
            select
                user_agent_id,
                case
                  when os_type = 'ios' then 'ios_app'
                  when os_type = 'android' then 'android_app'
                end as platform_type
            from dim.dim_fd_user_agent
            where is_app = 1
        ) t4 on t4.user_agent_id = t3.user_agent_id

        left join dim.dim_fd_region t5 on t5.region_id = t3.country
)reg_tab;
