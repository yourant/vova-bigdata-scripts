
INSERT OVERWRITE TABLE dwd.dwd_fd_common_module_interact PARTITION (pt = '${pt}')
select project,
       domain_userid,
       session_id,
       event_name,
       IF(event_name = 'common_impression', 'module_impression', 'module_click') as event_step,
       platform_type,
       country,
       app_version,
       REGEXP_EXTRACT(element_event_struct.element_id, 'm_jump=(.*?)(&.*?)?$', 1)     as module_name,
       ''
from ods_fd_snowplow.ods_fd_snowplow_element_event
where pt ='${pt}' and event_name in ('common_impression', 'common_click')
and REGEXP_EXTRACT(element_event_struct.element_id, 'm_jump=(.*?)(&.*?)?$', 1) <> '';


INSERT INTO TABLE dwd.dwd_fd_common_module_interact PARTITION (pt = '${pt}')
select project,
       domain_userid,
       session_id,
       event_name,
       IF(event_name = 'goods_impression', 'module_impression', 'module_click') as event_step,
       platform_type,
       country,
       app_version,
       REGEXP_EXTRACT(goods_event_struct.list_type, 'm_jump=(.*?)(&.*?)?$', 1)       as module_name,
       goods_event_struct.virtual_goods_id                                     as goods_id
from ods_fd_snowplow.ods_fd_snowplow_goods_event
where pt ='${pt}' and event_name in ('goods_impression', 'goods_click')
and REGEXP_EXTRACT(goods_event_struct.list_type, 'm_jump=(.*?)(&.*?)?$', 1) != '';


INSERT INTO TABLE dwd.dwd_fd_common_module_interact PARTITION (pt = '${pt}')
select project,
       domain_userid,
       session_id,
       event_name,
       'module_pv' as event_step,
       platform_type,
       country,
       app_version,
       REGEXP_EXTRACT(page_url, 'm_jump=(.*?)(&.*?)?$', 1) as module_name,
       url_virtual_goods_id as goods_id
from ods_fd_snowplow.ods_fd_snowplow_view_event
where pt ='${pt}'
  and event_name in ( 'page_view'
    , 'screen_view' )
  and REGEXP_EXTRACT(page_url
    , 'm_jump=(.*?)(&.*?)?$'
    , 1) != '';


INSERT INTO TABLE dwd.dwd_fd_common_module_interact PARTITION (pt = '${pt}')
select project,
       domain_userid,
       session_id,
       event_name,
       event_name as event_step,
       platform_type,
       country,
       app_version,
       REGEXP_EXTRACT(page_url, 'm_jump=(.*?)(&.*?)?$', 1) as module_name,
       ecommerce_product.id  as goods_id
from ods_fd_snowplow.ods_fd_snowplow_ecommerce_event
where pt ='${pt}'
and   event_name ='add'
and   REGEXP_EXTRACT(page_url, 'm_jump=(.*?)(&.*?)?$', 1) != '';


INSERT INTO TABLE dwd.dwd_fd_common_module_interact PARTITION (pt = '${pt}')
select project,
       domain_userid,
       session_id,
       event_name,
       event_name as event_step,
       platform_type,
       country,
       app_version,
       REGEXP_EXTRACT(page_url, 'm_jump=(.*?)(&.*?)?$', 1) as module_name,
       goods_event_struct.virtual_goods_id as goods_id
from ods_fd_snowplow.ods_fd_snowplow_goods_event
where  pt ='${pt}'
and (event_name ='goods_click' and goods_event_struct.list_type='list-category')
or (event_name ='goods_impression' and goods_event_struct.list_type='list-category')
and REGEXP_EXTRACT(page_url, 'm_jump=(.*?)(&.*?)?$', 1) != '';
