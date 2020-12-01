drop table tmp.tmp_fd_common_ctr;
create table tmp.tmp_fd_common_ctr as
SELECT
/*+ REPARTITION(10) */
event_name,
nvl(platform_type,'NALL'),
nvl(country,'NALL'),
nvl(project,'NALL'),
nvl(page_code,'NALL'),
nvl(element_event_struct.list_type,'NALL') list_name,
nvl(element_event_struct.element_name,'NALL') element_name,
session_id
from ods_fd_snowplow.ods_fd_snowplow_element_event
where pt='${pt}'
and event_name in ('common_impression', 'common_click')
and session_id is not null
group by platform_type,country,project,page_code,element_event_struct.list_type,element_event_struct.element_name,session_id,event_name;



insert overwrite table dwb.dwb_fd_common_ctr_rpt  partition(pt='${pt}')
SELECT
/*+ REPARTITION(1) */
nvl(platform_type,'all'),
nvl(country,'all'),
nvl(project,'all'),
nvl(page_code,'all'),
nvl(list_name,'all') AS list_name,
nvl(element_name,'all') AS element_name,
count(distinct if(event_name = 'common_impression', session_id, NULL)) AS impression_uv,
count(distinct if(event_name = 'common_click', session_id, NULL)) AS click_uv
from tmp.tmp_fd_common_ctr
group by platform_type,country,project,page_code,list_name,element_name with cube;