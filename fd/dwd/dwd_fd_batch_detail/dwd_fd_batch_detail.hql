
insert overwrite table dwd.dwd_fd_batch_detail partition(pt='${pt}')

select
    nvl(batch, "NALL")            as batch,
    nvl(virtual_goods_id, "NALL") as virtual_goods_id,
    nvl(project, "NALL")          as project,
    nvl(country, "NALL")          as country,
    nvl(platform_type, "NALL")    as platform_type,
    event,
    session_id
from
(
  select
    get_json_object(goods_event_struct.extra, '$.element_batch') as batch,
    goods_event_struct.virtual_goods_id                          as virtual_goods_id,
    project,
    country,
    platform_type,
    "goods_impression"                                           as event,
    session_id
  from  ods_fd_snowplow.ods_fd_snowplow_goods_event
  where pt='${pt}'
  and event_name = "goods_impression"
  and goods_event_struct.list_type = '/InspiredList'
  and length(country)<=2

  union all

  select
    get_json_object(element_event_struct.extra, '$.element_batch') as batch,
    element_event_struct.element_id                                as virtual_goods_id,
    project,
    country,
    platform_type,
    case lower(element_event_struct.element_name)
    when lower("InspiredGoodsLike") then "goods_like"
    when lower("InspiredGoodsDisLike") then "goods_dislike"
    else "null"
    end                                                        as event,
    session_id
  from ods_fd_snowplow.ods_fd_snowplow_element_event
  where pt='${pt}'
  and event_name in ("common_click")
  and lower(element_event_struct.element_name) in
  (lower("InspiredGoodsLike"), lower("InspiredGoodsDisLike"))
  and length(country)<=2
)t1;