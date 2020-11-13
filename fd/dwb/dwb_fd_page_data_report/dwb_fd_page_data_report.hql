create view if not exists dwb.dwb_fd_page_data_report
as select
       project,
       platform,
       platform_type,
       dvce_type,
       os_name,
       country,
       app_version,
       case
            when session_idx=1 then 'new' 
            when session_idx>1 then 'old'
            end  as is_new_user,
       page_code,
       session_id,
       dt
from ods.ods_fd_snowplow_all_event
where event_name in('page_view','screen_view')
