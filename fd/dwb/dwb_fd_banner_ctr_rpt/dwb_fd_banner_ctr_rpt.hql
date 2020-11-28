
insert overwrite table dwb.dwb_fd_banner_ctr_rpt partition (pt='${pt}')
select project,
       platform,
       country,
       app_version,
       dvce_type,
       list_type,
       element_name,
       absolute_position,
       click_session_id,
       impression_session_id
from (
         select project,
                platform,
                country,
                app_version,
                dvce_type,
                element_event_struct.list_type                               as list_type,
                element_event_struct.element_name,
                element_event_struct.absolute_position,
                if(event_name = 'common_click', session_id, null)      as click_session_id,
                if(event_name = 'common_impression', session_id, null) as impression_session_id
         from ods_fd_snowplow.ods_fd_snowplow_element_event 
         where pt = '${pt}'
           and lower(page_code) = 'homepage'
           and event_name in ('common_click', 'common_impression')
     ) tab1
where list_type regexp 'banner';
