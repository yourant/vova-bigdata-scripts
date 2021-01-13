insert overwrite table ads.ads_fd_goods_inspired partition (pt = '${pt}')
select /*+ REPARTITION(1) */
    batch,
    goods_id,
    virtual_goods_id,
    project,
    country,
    platform_name,
    count(distinct if(event = 'goods_like', session_id, null))       as like_num,
    count(distinct if(event = 'goods_dislike', session_id, null))    as unlike_num,
    count(distinct if(event = 'goods_impression', session_id, null)) as impressions
from (
         select batch            as batch,
                g.goods_id       as goods_id,
                t1.virtual_goods_id as virtual_goods_id,
                project          as project,
                country          as country,
                case
                    when platform_type in ('pc_web', 'tablet_web') then 'PC'
                    when platform_type in ('mobile_web') then 'H5'
                    when platform_type in ('ios_app', 'android_app') then 'APP'
                    else 'others'
                    end          as platform_name,
                event,
                session_id
         from (
                  select get_json_object(goods_event_struct.extra, '$.element_batch') as batch,
                         goods_event_struct.virtual_goods_id                          as virtual_goods_id,
                         project,
                         country,
                         platform_type,
                         'goods_impression'                                           as event,
                         session_id
                  from ods_fd_snowplow.ods_fd_snowplow_goods_event
                  where pt = '${pt}'
                    and event_name = 'goods_impression'
                    and goods_event_struct.list_type = '/InspiredList'
                  union all
                  select get_json_object(element_event_struct.extra, '$.element_batch') as batch,
                         element_event_struct.element_id                                as virtual_goods_id,
                         project,
                         country,
                         platform_type,
                         case lower(element_event_struct.element_name)
                             when lower('InspiredGoodsLike') then 'goods_like'
                             when lower('InspiredGoodsDisLike') then 'goods_dislike'
                             else 'null'
                             end                                                        as event,
                         session_id
                  from ods_fd_snowplow.ods_fd_snowplow_element_event
                  where pt = '${pt}'
                    and event_name in ('common_click')
                    and lower(element_event_struct.element_name) in
                        (lower('InspiredGoodsLike'), lower('InspiredGoodsDisLike'))
              ) t1
                  left join dim.dim_fd_goods g on t1.virtual_goods_id = g.virtual_goods_id
     ) t2
     where t2.batch >= '${batchNum}'
group by batch, goods_id, virtual_goods_id, project, country, platform_name
having length(country) <=2