drop table tmp.tmp_fd_page_data_rpt;
create table tmp.tmp_fd_page_data_rpt as
SELECT
/*+ REPARTITION(5) */
nvl(project,'NALL'),
nvl(country,'NALL'),
nvl(platform_type,'NALL'),
nvl(os_name,'NALL'),
nvl(app_version,'NALL'),
case
       when platform = 'web' and session_idx = 1 then 'new'
       when platform = 'web' and session_idx > 1 then 'old'
       when platform = 'mob' and session_idx = 1 then 'new'
       when platform = 'mob' and session_idx > 1 then 'old'
end  as is_new_user,
nvl(page_code,'NALL'),
session_id
from ods_fd_snowplow.ods_fd_snowplow_view_event
where pt='${pt}'
and session_id is not null;


insert overwrite table  dwb.dwb_fd_page_data_rpt partition (pt='${pt}')

select  /*+ REPARTITION(1) */
       project,
       country,
       platform_type,
       os_name,
       app_version,
       is_new_user,
       page_code,
       count(session_id),
       count(distinct session_id)
from tmp.tmp_fd_page_data_rpt
group by  project,
                country,
                platform_type,
                os_name,
                app_version,
                is_new_user,
                page_code with cube;
