insert overwrite table dwb.dwb_fd_120_position_impression_uv_7d
select
/*+ REPARTITION(1) */
       project,
       platform_name,
       route_sn,
       route_name,
       country,
       absolute_position,
       count(distinct session_id) as impression_uv,
       "${pt}"
from (
         select project,
                case
                    when platform_type in ('pc_web', 'tablet_web') then 'PC'
                    when platform_type in ('mobile_web') then 'H5'
                    when platform_type in ('ios_app', 'android_app') then 'APP'
                    else 'others'
                    end                                      as platform_name,
                url_route_sn as route_sn,
                case
                    when url_route_sn = '9872' then 'Dress'
                    when url_route_sn = '9851' then 'Blouse'
                    when url_route_sn = '9865' then 'Swimwear'
                    when url_route_sn = '9893' then 'Shoes'
                    when url_route_sn = '9850' then 'T-shirts'
                    when url_route_sn = '8688' then 'Bottoms'
                    when url_route_sn = '3767' then 'Plus Dress'
                    when url_route_sn = '1476' then 'plus Blouses'
                    when url_route_sn = '8580' then 'Plus Swimwear'
                    when url_route_sn = '1101' then 'Dress'
                    when url_route_sn = '1107' then 'Blouse'
                    when url_route_sn = '9991' then 'Swimwear'
                    when url_route_sn = '1104' then 'Shoes'
                    else 'others'
                    end                                      as route_name,
                upper(country)                               as country,
                cast(goods_event_struct.absolute_position as bigint) as absolute_position,
                session_id
         from ods_fd_snowplow.ods_fd_snowplow_goods_event
         where pt between date_sub('${pt}', 6) and '${pt}'
           and event_name = 'goods_impression'
           and goods_event_struct.list_type = 'list-category'
           and length(country) = 2
     ) t
where absolute_position between 1 and 120
group by project, platform_name, route_sn, route_name, country, absolute_position;