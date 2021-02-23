set hive.new.job.grouping.set.cardinality=128;
insert overwrite table dwb.dwb_fd_rpt_main_process_rpt PARTITION (pt = '${pt}')
select /*+ REPARTITION(1) */ session_table.project
     , session_table.platform_type
     , session_table.country
     , session_table.is_new_user
     , session_table.ga_channel

     , nvl(session_table.all_sessions,0)
     , nvl(session_table.product_view_sessions,0)
     , nvl(session_table.add_sessions,0)
     , nvl(session_table.checkout_sessions,0)
     , nvl(session_table.checkout_option_sessions,0)
     , nvl(session_table.purchase_sessions,0)

     , nvl(order_table.orders,0)
     , nvl(order_table.goods_amount,0)
     , nvl(order_table.bonus,0)
     , nvl(order_table.shipping_fee,0)
     , nvl(session_table.pv_session,0)
from (
         select nvl(t1.country, 'all')                          as country
              , nvl(t1.project, 'all')                          as project
              , nvl(t1.platform_type, 'all')                    as platform_type
              , nvl(t1.is_new_user, 'all')                      as is_new_user
              , nvl(t1.ga_channel, 'all')                       as ga_channel
              , count(distinct (t1.session_id))                 as all_sessions
              , count(distinct (t1.product_view_session_id))    as product_view_sessions
              , count(distinct (t1.add_session_id))             as add_sessions
              , count(distinct (t1.checkout_session_id))        as checkout_sessions
              , count(distinct (t1.checkout_option_session_id)) as checkout_option_sessions
              , count(distinct (t1.purchase_session_id))        as purchase_sessions
              , count(distinct (t1.pv_session_id))              as pv_session
         from (
                  select nvl(fms.country, 'others')      as country
                       , fms.project                     as project
                       , nvl(fms.platform_type,'others')  as platform_type
                       , nvl(fms.is_new_user, 'old')     as is_new_user
                       , case
                            when lower(fdusc.ga_channel) = 'others' then 'others'
                            when length(fdusc.ga_channel) > 1 and lower(fdusc.ga_channel) != 'others' then fdusc.ga_channel
                         else 'others' end as ga_channel
                       , fms.session_id
                       , fms.product_view_session_id
                       , fms.add_session_id
                       , fms.checkout_session_id
                       , fms.checkout_option_session_id
                       , fms.purchase_session_id
                       , if(fms.event_name in ('page_view', 'screen_view') and fms.view_event_sum = 1 and fms.all_event_sum = fms.view_event_sum,fms.session_id,null) as pv_session_id
                  from (
                        select lower(project) as project
                            , upper(country) as country
                            , platform_type
                            , case
                                when platform = 'web' and session_idx = 1 then 'new'
                                when platform = 'web' and session_idx > 1 then 'old'
                                when platform = 'mob' and session_idx = 1 then 'new'
                                when platform = 'mob' and session_idx > 1 then 'old'
                                end as is_new_user
                            , session_id
                            , event_name
                            , if(event_name in ('page_view', 'screen_view') and page_code = 'product', session_id,NULL) as product_view_session_id
                            , if(event_name in ('add'), session_id, NULL)             as add_session_id
                            , if(event_name in ('checkout'), session_id, NULL)        as checkout_session_id
                            , if(event_name in ('checkout_option'), session_id, NULL) as checkout_option_session_id
                            , if(event_name in ('purchase'), session_id, NULL)        as purchase_session_id
                            , if(event_name in ('page_view', 'screen_view'),sum(1) over(partition by project,event_name,session_id),0) as view_event_sum
                            , sum(1) over(partition by project,session_id) as all_event_sum
                        from ods_fd_snowplow.ods_fd_snowplow_all_event
                        where pt = '${pt}'
                            and project is not null
                            and length(project) > 2
                            and length(country) = 2
                            and event_name not in ('common_impression', 'goods_impression')

                        union all

                        select
                            'website_cluster' as project
                            , upper(country) as country
                            , platform_type
                            , case
                                when platform = 'web' and session_idx = 1 then 'new'
                                when platform = 'web' and session_idx > 1 then 'old'
                                when platform = 'mob' and session_idx = 1 then 'new'
                                when platform = 'mob' and session_idx > 1 then 'old'
                                end as is_new_user
                            , session_id
                            , event_name
                            , if(event_name in ('page_view', 'screen_view') and page_code = 'product', session_id,NULL) as product_view_session_id
                            , if(event_name in ('add'), session_id, NULL)             as add_session_id
                            , if(event_name in ('checkout'), session_id, NULL)        as checkout_session_id
                            , if(event_name in ('checkout_option'), session_id, NULL) as checkout_option_session_id
                            , if(event_name in ('purchase'), session_id, NULL)        as purchase_session_id
                            , if(event_name in ('page_view', 'screen_view'),sum(1) over(partition by project,event_name,session_id),0) as view_event_sum
                            , sum(1) over(partition by project,session_id) as all_event_sum
                        from ods_fd_snowplow.ods_fd_snowplow_all_event
                        where pt = '${pt}'
                          and project is not null
                          and length(project) > 2
                          and length(country) = 2
                          and event_name not in ('common_impression', 'goods_impression')
                          and lower(project) in ('beautlly','merecloth','cosydress', 'baltershop', 'chichut', 'joycedays', 'blessrose', 'shinynight', 'vividpretty', 'jollyweek', 'eoschoice', 'cherbow', 'cherlady')

                    ) fms
                    left join (
                        select
                            session_id, collect_set(ga_channel)[0] as ga_channel
                        from dwd.dwd_fd_session_channel
                        where pt BETWEEN date_sub('${pt}', 2) AND date_add('${pt}', 1)
                        group by session_id
                    ) fdusc on fms.session_id = fdusc.session_id

              ) t1
         group by t1.country, t1.project, t1.platform_type, t1.is_new_user, t1.ga_channel with cube

     ) session_table
         left join
     (
         select nvl(t2.country, 'all')        as country
              , nvl(t2.project_name, 'all')   as project
              , nvl(t2.platform_type, 'all')  as platform_type
              , nvl(t2.is_new_user, 'all')    as is_new_user
              , nvl(t2.ga_channel, 'all')     as ga_channel
              , count(distinct (t2.order_id)) as orders
              , sum(t2.goods_amount)          as goods_amount
              , sum(t2.bonus)                 as bonus
              , sum(t2.shipping_fee)          as shipping_fee
         from (
                  select oi.project_name
                       , nvl(oi.country_code, 'others')    as country
                       , nvl(oi.platform_type,'others')    as platform_type
                       ,case
                           when lower(fdpsc.ga_channel) = 'others' then 'others'
                           when length(fdpsc.ga_channel) > 1 and lower(fdpsc.ga_channel) != 'others' then fdpsc.ga_channel
                       else 'others' end as ga_channel
                       , nvl(fms.is_new_user, 'old')       as is_new_user

                       , oi.order_id
                       , oi.goods_amount
                       , oi.bonus
                       , oi.shipping_fee
                  from (
                        select
                            lower(project_name) as project_name,
                            country_code,
                            platform_type,
                            order_id,
                            goods_amount,
                            bonus,
                            shipping_fee
                        from dwd.dwd_fd_order_info oi
                        where date(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss')) = '${pt}'
                        and pay_status = 2
                        and project_name is not NULL
                        and length(project_name) > 2
                  ) oi
                  left join (select order_id,sp_session_id from ods_fd_vb.ods_fd_order_marketing_data group by order_id,sp_session_id) om on om.order_id = oi.order_id
                  left join (

                      select soe.session_id,collect_set(soe.is_new_user)[0] as is_new_user
                        from (
                            select
                                session_id,
                                case
                                when platform = 'web' and session_idx = 1 then 'new'
                                when platform = 'web' and session_idx > 1 then 'old'
                                when platform = 'mob' and session_idx = 1 then 'new'
                                when platform = 'mob' and session_idx > 1 then 'old'
                                end as is_new_user
                            from ods_fd_snowplow.ods_fd_snowplow_all_event
                            where pt BETWEEN date_sub('${pt}', 4) AND '${pt}' and event_name in ('page_view', 'screen_view')
                        ) soe group by soe.session_id

                  ) fms on fms.session_id = om.sp_session_id
                  left join (
                       select
                            session_id,
                            collect_set(ga_channel)[0] as ga_channel
                        from dwd.dwd_fd_session_channel
                        where pt BETWEEN date_sub('${pt}', 2) AND date_add('${pt}', 1)
                        group by session_id

                  ) fdpsc on fdpsc.session_id = om.sp_session_id

              ) t2
         group by t2.country, t2.project_name, t2.platform_type, t2.is_new_user, t2.ga_channel with cube

         union all

         select wsc.country,
            wsc.project,
            wsc.platform_type,
            wsc.is_new_user,
            wsc.ga_channel,
            wsc.orders,
            wsc.goods_amount,
            wsc.bonus,
            wsc.shipping_fee
         from (
          select nvl(t2.country, 'all')        as country
                , nvl(t2.project_name, 'all')   as project
                , nvl(t2.platform_type, 'all')  as platform_type
                , nvl(t2.is_new_user, 'all')    as is_new_user
                , nvl(t2.ga_channel, 'all')     as ga_channel
                , count(distinct (t2.order_id)) as orders
                , sum(t2.goods_amount)          as goods_amount
                , sum(t2.bonus)                 as bonus
                , sum(t2.shipping_fee)          as shipping_fee
           from (
                    select oi.project_name
                         , nvl(oi.country_code, 'others')    as country
                         , nvl(oi.platform_type,'others')    as platform_type
                         ,case
                             when lower(fdpsc.ga_channel) = 'others' then 'others'
                             when length(fdpsc.ga_channel) > 1 and lower(fdpsc.ga_channel) != 'others' then fdpsc.ga_channel
                         else 'others' end as ga_channel
                         , nvl(fms.is_new_user, 'old')       as is_new_user

                         , oi.order_id
                         , oi.goods_amount
                         , oi.bonus
                         , oi.shipping_fee
                    from (
                          select
                              'website_cluster' as project_name,
                              country_code,
                              platform_type,
                              order_id,
                              goods_amount,
                              bonus,
                              shipping_fee
                          from dwd.dwd_fd_order_info oi
                          where date(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss')) = '${pt}'
                          and pay_status = 2
                          and project_name is not NULL
                          and lower(project_name) in ('beautlly','merecloth','cosydress', 'baltershop', 'chichut', 'joycedays', 'blessrose', 'shinynight', 'vividpretty', 'jollyweek', 'eoschoice', 'cherbow', 'cherlady')
                          and length(project_name) > 2

                    ) oi
                    left join (select order_id,sp_session_id from ods_fd_vb.ods_fd_order_marketing_data group by order_id,sp_session_id) om on om.order_id = oi.order_id
                    left join (

                        select soe.session_id,collect_set(soe.is_new_user)[0] as is_new_user
                          from (
                              select
                                  session_id,
                                  case
                                  when platform = 'web' and session_idx = 1 then 'new'
                                  when platform = 'web' and session_idx > 1 then 'old'
                                  when platform = 'mob' and session_idx = 1 then 'new'
                                  when platform = 'mob' and session_idx > 1 then 'old'
                                  end as is_new_user
                              from ods_fd_snowplow.ods_fd_snowplow_all_event
                              where pt BETWEEN date_sub('${pt}', 4) AND '${pt}' and event_name in ('page_view', 'screen_view')
                          ) soe group by soe.session_id

                    ) fms on fms.session_id = om.sp_session_id
                    left join (
                         select
                              session_id,
                              collect_set(ga_channel)[0] as ga_channel
                          from dwd.dwd_fd_session_channel
                          where pt BETWEEN date_sub('${pt}', 2) AND date_add('${pt}', 1)
                          group by session_id

                    ) fdpsc on fdpsc.session_id = om.sp_session_id

                ) t2
           group by t2.country, t2.project_name, t2.platform_type, t2.is_new_user, t2.ga_channel with cube

         )wsc where wsc.project != 'all'

     ) order_table on session_table.country = order_table.country
         and session_table.project = order_table.project
         and session_table.platform_type = order_table.platform_type
         and session_table.is_new_user = order_table.is_new_user
         and session_table.ga_channel = order_table.ga_channel;