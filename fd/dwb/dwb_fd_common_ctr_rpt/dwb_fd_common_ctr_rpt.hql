drop table tmp.tmp_fd_common_ctr;
create table tmp.tmp_fd_common_ctr as
SELECT
/*+ REPARTITION(10) */
event_name,
platform_type,
country,
project,
page_code,
element_event_struct.list_type list_name,
element_event_struct.element_name element_name,
session_id
from ods_fd_snowplow.ods_fd_snowplow_element_event
where pt='2020-11-28'
and event_name in ('common_impression', 'common_click')
and platform_type is not null
and country is not null
and project is not null
and page_code is not null
and element_event_struct.list_type is not null
and element_event_struct.element_name is not null
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