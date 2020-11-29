
insert overwrite table dwb.dwb_fd_banner_ctr_rpt partition (pt='${pt}')

select
        nvl(project,'all'),
        nvl(platform,'all'),
        nvl(country,'all'),
        nvl(app_version,'all'),
        nvl(dvce_type,'all'),
        nvl(list_type,'all'),
        nvl(absolute_position,'all'),
       count(distinct click_session_id),
       count(distinct impression_session_id)
from (
         select nvl(project,'NALL') as project,
                nvl(platform,''NALL) as platform,
                nvl(country,'NALL') as country,
                nvl(app_version,'NALL') as app_version,
                nvl(dvce_type,'NALL')as dvce_type,
                nvl(element_event_struct.list_type,'NALL')                               as list_type,
                nvl(element_event_struct.element_name,'NALL') as element_name,
                nvl(cast(element_event_struct.absolute_position as String) ,'NALL')    as absolute_position,
                if(event_name = 'common_click', session_id, null)          as click_session_id,
                if(event_name = 'common_impression', session_id, null)     as impression_session_id
         from ods_fd_snowplow.ods_fd_snowplow_element_event 
         where pt = '${pt}'
           and lower(page_code) = 'homepage'
           and event_name in ('common_click', 'common_impression')
           and element_event_struct.list_type regexp 'banner'
     ) tab1
group by project,platform,country,app_version,dvce_type,list_type,absolute_position with cube
;
