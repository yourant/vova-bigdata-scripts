
insert overwrite table dwb.dwb_fd_common_ctr_rpt  partition(pt='${pt}')

select
    nvl(platform_type,'all'),
    nvl(app_version,'all'),
    nvl(country,'all'),
    nvl(language,'all'),
    nvl(project,'all'),
    nvl(page_code,'all'),
    nvl(position,'all'),
    nvl(list_name,'all'),
    nvl(element_name,'all'),
    nvl(element_content,'all'),
    nvl(element_type,'all'),
    count(distinct impression_session_id),
    count(distinct click_session_id)

from(
SELECT
           nvl(platform_type,'NALL') as platform_type,
           nvl(app_version,'NALL') as app_version,
           nvl(country,'NALL') as country,
           nvl(`language`,'NALL') as language,
           nvl(project,'NALL') as project,
           nvl(page_code,'NALL') as page_code,
          nvl(cast(element_event_struct.absolute_position as String),'NALL') AS position,
          nvl(element_event_struct.list_type,'NALL')   AS list_name,
          nvl(element_event_struct.element_name,'NALL')  AS element_name,
          nvl(element_event_struct.element_content,'NALL')  AS element_content,
          nvl(element_event_struct.element_type ,'NALL') AS element_type,
          IF(event_name = 'common_impression', session_id, NULL) AS impression_session_id,
          IF(event_name = 'common_click', session_id, NULL) AS click_session_id
from ods_fd_snowplow.ods_fd_snowplow_element_event
where event_name in ('common_impression', 'common_click')
and country is not null
and pt='${pt}'

)tab1
group by platform_type,app_version,country,language,project,page_code,
           position,list_name,element_name,element_content,element_type with cube
            ;

