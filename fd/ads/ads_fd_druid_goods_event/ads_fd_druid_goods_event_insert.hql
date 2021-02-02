SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;

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
                og.goods_id,
                gi.cat_id,
                gi.virtual_goods_id,
                oi.order_time,
                pe.event_time as paying_time,
                oi.pay_time,
                oi.order_id

         from ods_fd_vb.ods_fd_order_info_h oi
                  inner join ods_fd_vb.ods_fd_order_goods_h og on oi.order_id = og.order_id
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
         where oi.email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
     )
insert overwrite table ads.ads_fd_druid_goods_event partition (pt = '${pt}', hour = '${hour}')
select
    /*+ REPARTITION(5) */
    tmp.event_time,
    tmp.record_type,
    tmp.project,
    tmp.domain_userid,
    tmp.session_id,
    tmp.platform_type,
    tmp.country,
    cat_id,
    goods_id,
    tmp.virtual_goods_id,
    tmp.page_code,
    tmp.list_type,
    tmp.absolute_position,
    tmp.url_route_sn,
    tmp.event_num,
    tmp.order_num,
    tmp.paying_order_num,
    tmp.paid_order_num,
    order_id
from (
         -- click impression
         select collector_ts                         as `event_time`,
                case event_name
                    when "goods_click" then "click"
                    when "goods_impression" then "impression"
                    end
                                                     as `record_type`,
                project,
                domain_userid,
                session_id,
                platform_type,
                country,
                gi.cat_id,
                gi.goods_id,
                goods_event_struct.virtual_goods_id  as virtual_goods_id,
                page_code,
                goods_event_struct.list_type         as list_type,
                goods_event_struct.absolute_position as absolute_position,
                url_route_sn,
                1                                    as event_num,
                0                                    as order_num,
                0                                    as paying_order_num,
                0                                    as paid_order_num,
                null                                 as order_id
         from ods_fd_snowplow.ods_fd_snowplow_goods_event sp_ge
                  left join goods_info gi on gi.virtual_goods_id = sp_ge.goods_event_struct.virtual_goods_id
         where pt = '${pt}'
            and hour = '${hour}'


         union all

         -- detail_view
         select collector_ts         as `event_time`,
                "detail_view"        as `record_type`,
                project,
                domain_userid,
                session_id,
                platform_type,
                country,
                gi.cat_id,
                gi.goods_id,
                url_virtual_goods_id as virtual_goods_id,
                page_code,
                null                 as list_type,
                null                 as absolute_position,
                url_route_sn,
                1                    as event_num,
                0                    as order_num,
                0                    as paying_order_num,
                0                    as paid_order_num,
                null                 as order_id
         from ods_fd_snowplow.ods_fd_snowplow_view_event sp_ve
                  left join goods_info gi on gi.virtual_goods_id = sp_ve.url_virtual_goods_id
         where pt = '${pt}'
           and hour = '${hour}'
           and page_code = 'product'


         union all
         -- 交易事件
         select collector_ts         as `event_time`,
                event_name           as `record_type`,
                project,
                domain_userid,
                session_id,
                platform_type,
                country,
                gi.cat_id,
                gi.goods_id,
                ecommerce_product.id as virtual_goods_id,
                page_code,
                null                 as list_type,
                null                 as absolute_position,
                url_route_sn,
                1                    as event_num,
                0                    as order_num,
                0                    as paying_order_num,
                0                    as paid_order_num,
                null                 as order_id
         from ods_fd_snowplow.ods_fd_snowplow_ecommerce_event sp_ee
                  left join goods_info gi on gi.virtual_goods_id = sp_ee.ecommerce_product.id
         where pt = '${pt}'
           and hour = '${hour}'
           and event_name in ("add", "remove", "checkout", "checkout_option", "purchase")


         union all
         -- paid订单
         select to_utc_timestamp(pay_time, 'PST') as `event_time`,
                'order'                                        as `record_type`,
                project_name                                   as project,
                null                                           as domain_userid,
                null                                           as session_id,
                platform_type,
                country_code                                   as country,
                cat_id,
                goods_id,
                virtual_goods_id,
                null                                           as page_code,
                null                                           as list_type,
                null                                           as absolute_position,
                null                                           as url_route_sn,
                0                                              as event_num,
                0                                              as order_num,
                0                                              as paying_order_num,
                1                                              as paid_order_num,
                order_id
         from order_goods
         where date(to_utc_timestamp(pay_time, 'PST')) = '${pt}'
         and hour(to_utc_timestamp(pay_time, 'PST')) = cast('${hour}' as bigint)
         union all
         -- 下单订单
         select to_utc_timestamp(order_time, 'PST') as `event_time`,
                'order'                                          as `record_type`,
                project_name                                     as project,
                null                                             as domain_userid,
                null                                             as session_id,
                platform_type,
                country_code                                     as country,
                cat_id,
                goods_id,
                virtual_goods_id,
                null                                             as page_code,
                null                                             as list_type,
                null                                             as absolute_position,
                null                                             as url_route_sn,
                0                                                as event_num,
                1                                                as order_num,
                0                                                as paying_order_num,
                0                                                as paid_order_num,
                order_id
         from order_goods
         where date(to_utc_timestamp(order_time, 'PST')) = '${pt}'
         and hour(to_utc_timestamp(order_time, 'PST')) = cast('${hour}' as bigint)

         union all
         -- 下单订单
         select to_utc_timestamp(paying_time, 'PST')       as `event_time`,
                'order'                                    as `record_type`,
                project_name                               as project,
                null                                       as domain_userid,
                null                                       as session_id,
                platform_type,
                country_code                               as country,
                cat_id,
                goods_id,
                virtual_goods_id,
                null                                       as page_code,
                null                                       as list_type,
                null                                       as absolute_position,
                null                                       as url_route_sn,
                0                                          as event_num,
                0                                          as order_num,
                1                                          as paying_order_num,
                0                                          as paid_order_num,
                order_id
         from order_goods
         where date(to_utc_timestamp(paying_time, 'PST')) = '${pt}'
    and hour(to_utc_timestamp(paying_time, 'PST')) = cast('${hour}' as bigint)
     ) tmp
;