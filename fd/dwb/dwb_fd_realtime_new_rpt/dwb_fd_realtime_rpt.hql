insert overwrite table dwb.dwb_fd_realtime_new_rpt partition (pt = '${pt}')
select
    /*+ REPARTITION(1) */
    nvl(project, 'all')                     as project,
    nvl(platform, 'all')                    as platform,
    nvl(country, 'all')                     as country,
    nvl(hour, 'all')                        as hour,
    sum(order_number)                       as order_number,
    sum(gmv)                                as gmv,
    sum(session_number)                     as session_number,
    sum(order_number) / sum(session_number) as conversion_rate,
    sum(goods_amount)                       as goods_amount
from (
         select project,
                platform,
                country,
                hour,
                count(*)          as order_number,
                sum(gmv)          as gmv,
                sum(goods_amount) as goods_amount,
                0                 as session_number
         from (
                  select hour(t1.paid_time)                as hour,
                         t1.project_name                   as project,
                         case
                             when t2.is_app = 0 and t2.device_type = 'pc' then 'PC'
                             when t2.is_app = 0 and t2.device_type = 'mobile' then 'H5'
                             when t2.is_app = 0 and t2.device_type = 'pad' then 'Tablet'
                             when t2.is_app = 1 and t2.os_type = 'android' then 'Android'
                             when t2.is_app = 1 and t2.os_type = 'ios' then 'IOS'
                             else 'Others'
                             end                           as platform,
                         t3.region_code                    as country,
                         t1.goods_amount + t1.shipping_fee as gmv,
                         t1.goods_amount                   as goods_amount
                  from (
                           select date_format(to_utc_timestamp(pay_time, 'America/Los_Angeles'),
                                              'yyyy-MM-dd HH:mm:ss') as paid_time,
                                  project_name,
                                  country,
                                  user_agent_id,
                                  goods_amount,
                                  shipping_fee
                           from ods_fd_vb.ods_fd_order_info_inc
                           where pt = '${pt}'
                             and pay_status = 2
                             and to_date(date_format(to_utc_timestamp(pay_time, 'America/Los_Angeles'),
                                                     'yyyy-MM-dd HH:mm:ss')) = '${pt}'
                             and hour(to_utc_timestamp(pay_time, 'America/Los_Angeles')) <= '${hour}'
                             and email NOT REGEXP '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
                       ) t1
                           left join ods_fd_vb.ods_fd_user_agent_analysis t2 on t1.user_agent_id = t2.user_agent_id
                           left join dim.dim_fd_region t3 on t1.country = t3.region_id
              ) t4
         group by project,
                  platform,
                  country,
                  hour
         with cube

         union all

         select project,
                platform,
                country,
                hour,
                0                          as order_number,
                0                          as gmv,
                0                          as goods_amount,
                count(distinct session_id) as session_number
         from (
                  select project,
                         case platform_type
                             when 'pc_web' then 'PC'
                             when 'mobile_web' then 'H5'
                             when 'android_app' then 'Android'
                             when 'ios_app' then 'IOS'
                             when 'tablet_web' then 'Tablet'
                             else 'Others'
                             end              as platform,
                         country,
                         session_id,
                         cast(hour as bigint) as hour
                  from ods_fd_snowplow.ods_fd_snowplow_view_event
                  where pt = '${pt}'
                    and session_id is not null
              ) t2
         group by project,
                  platform,
                  country,
                  hour
         with cube
     ) t
group by project,
         platform,
         country,
         hour
;
