SET hive.exec.compress.output=true;

with goods_info as (
    select g.goods_id, g.cat_id, vg.virtual_goods_id, lower(vg.project_name) as project_name
    from ods_fd_vb.ods_fd_goods_h g
             inner join ods_fd_vb.ods_fd_virtual_goods_h vg on vg.goods_id = g.goods_id
),
     order_goods as (
         select oi.project_name,
                case
                    when is_app = 0 and device_type = 'pc' then 'pc_web'
                    when is_app = 0 and device_type = 'mobile' then 'mobile_web'
                    when is_app = 0 and device_type = 'pad' then 'tablet_web'
                    when is_app = 1 and os_type = 'ios' then 'ios_app'
                    when is_app = 1 and os_type = 'android' then 'android_app'
                    else 'others'
                    end       as platform_type,
                r.region_code as country_code,
                oi.user_id,
                ud.sp_duid,
                dfl.language_code,
                og.goods_id,
                gi.cat_id,
                gi.virtual_goods_id,
                oi.order_time,
                pe.event_time as paying_time,
                oi.pay_time,
                oi.order_id,
                oi.pay_status,
                oi.order_sn

         from ods_fd_vb.ods_fd_order_info_h oi
                  inner join
                  (select order_id,goods_id from ods_fd_vb.ods_fd_order_goods_h group by order_id,goods_id) og on oi.order_id = og.order_id
                  left join ods_fd_vb.ods_fd_user_agent_analysis_h   uaa on uaa.user_agent_id = oi.user_agent_id
                  left join ods_fd_vb.ods_fd_region_h r on r.region_id = oi.country
                  left join goods_info gi on gi.goods_id = og.goods_id and lower(gi.project_name) = lower(oi.project_name)
                  left join (select order_sn, event_time
                             from (
                                      select order_sn,
                                             create_time                                           as event_time,
                                             row_number() over (partition by order_sn order by id) as rn
                                      from ods_fd_vb.ods_fd_order_status_change_history_h
                                      where field_name = 'pay_status'
                                        and old_value = 0
                                        and new_value in (1, 2)
                                  ) pay_status_change_log
                             where rn = 1
         ) pe on pe.order_sn = oi.order_sn
         left join dim.dim_fd_language dfl on oi.language_id = dfl.language_id
                  left join (
         select du.user_id, du.sp_duid
             from (
                      select user_id, sp_duid, row_number() OVER (PARTITION BY user_id ORDER BY last_update_time DESC) AS rank
                      from ods_fd_vb.ods_fd_user_duid
                      where sp_duid IS NOT NULL
                  ) du
             where du.rank = 1
         ) ud ON oi.user_id = ud.user_id
         where oi.email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
     )

insert overwrite table dwd.dwd_fd_goods_order
select
    /*+ REPARTITION(20) */
    trim(lower(tmp.project)) as project,
    tmp.platform,
    trim(upper(tmp.country)) as country,
    tmp.language,
    tmp.cat_id,
    tmp.goods_id,
    tmp.order_num,
    tmp.paid_order_num
from
    (
     select
            project_name                                   as project,
            if(platform_type='mobile_web', 'h5',if(platform_type in ('android_app', 'ios_app'),'mob',if(platform_type in ('tablet_web','pc_web'),'web','other'))) as platform,
            country_code                                   as country,
            language_code                                  as language,
            cat_id,
            goods_id,
            0                                              as order_num,
            1                                              as paid_order_num
     from order_goods
     where
     ((date(to_utc_timestamp(pay_time, 'PST')) = '${pt_begin}'
     and hour(to_utc_timestamp(pay_time, 'PST')) >= cast('16' as bigint))
     or
     (date(to_utc_timestamp(pay_time, 'PST')) = '${pt_end}'
     and hour(to_utc_timestamp(pay_time, 'PST')) < cast('16' as bigint))
     or
     (date(to_utc_timestamp(pay_time, 'PST')) > '${pt_begin}'
     and date(to_utc_timestamp(pay_time, 'PST')) < '${pt_end}'))
     and
     pay_status = 2 and length(country_code) < 3 and goods_id is not null

     union all

     select
            project_name                                     as project,
            if(platform_type='mobile_web', 'h5',if(platform_type in ('android_app', 'ios_app'),'mob',if(platform_type in ('tablet_web','pc_web'),'web','other'))) as platform,
            country_code                                     as country,
            language_code                                    as language,
            cat_id,
            goods_id,
            1                                                as order_num,
            0                                                as paid_order_num
     from order_goods
     where
     ((date(to_utc_timestamp(order_time, 'PST')) = '${pt_begin}'
     and hour(to_utc_timestamp(order_time, 'PST')) >= cast('16' as bigint))
     or
     (date(to_utc_timestamp(order_time, 'PST')) = '${pt_end}'
     and hour(to_utc_timestamp(order_time, 'PST')) < cast('16' as bigint))
     or
     (date(to_utc_timestamp(order_time, 'PST')) > '${pt_begin}'
     and date(to_utc_timestamp(order_time, 'PST')) < '${pt_end}')) and length(country_code) < 3 and goods_id is not null
) tmp
;
