
insert overwrite table  dwb.dwb_fd_page_data_rpt partition (pt='${pt}')
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
where pt='${pt}' and event_name in('page_view','screen_view');
