
insert overwrite table  dwb.dwb_fd_page_data_rpt partition (pt='${pt}')
select
    nvl(project,'all'),
    nvl(platform,'all'),
    nvl(platform_type,'all'),
    nvl(dvce_type,'all'),
    nvl(os_name,'all'),
    nvl(country,'all'),
    nvl(app_version,'all'),
    nvl(is_new_user,'all'),
    nvl(page_code,'all'),
    count(1),
    count(distinct session_id)

from(
select
       project,
       platform,
       platform_type,
       dvce_type,
       os_name,
       country,
       app_version,
       case
                when platform = 'web' and session_idx = 1 then 'new'
                when platform = 'web' and session_idx > 1 then 'old'
                when platform = 'mob' and session_idx = 1 then 'new'
                when platform = 'mob' and session_idx > 1 then 'old'
       end  as is_new_user,
       page_code,
       session_id
from ods_fd_snowplow.ods_fd_snowplow_all_event
where pt='${pt}' and event_name in('page_view','screen_view')

)tab1 group by        project,
                      platform,
                      platform_type,
                      dvce_type,
                      os_name,
                      country,
                      app_version,
                      is_new_user,
                      page_code with cube;
