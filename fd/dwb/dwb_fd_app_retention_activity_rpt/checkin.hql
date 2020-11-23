insert overwrite table dwb.dwb_fd_app_retention_activity partition (pt='${pt}',classify='checkin')
select
project as project,
platform_type as platform_type,
country as country_code,
domain_userid as all_domain_userid,
null,
null,null,null,null,null,null,null,null,
if(page_code = 'myrewards',domain_userid,null) as checkin_points_domain_userid,
null as checkin_userid,
null as checkin_userid_first,
null,null,null,null,
null,null,null,null,null,null,
null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
null,null,null,null,null,null,
null,null,null,null
from ods_fd_snowplow.ods_fd_snowplow_all_event
where pt = '${pt}' and platform_type in ('android_app','ios_app') and page_code = 'myrewards'
and project is not null and project !='' and country is not null and country != ''

union
select
ul.project as project,
case when ul.type = 'android_ap' then 'android_app'
 when ul.type = 'ios_app' then 'ios_app' end as platform_type,
r.region_code as country_code,
null as all_domain_userid,
null,
null,null,null,null,null,null,null,null,
null as checkin_points_domain_userid,
ul.user_id as checkin_userid,
null as checkin_userid_first,
null,null,null,null,
null,null,null,null,null,null,
null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
null,null,null,null,null,null,
null,null,null,null
from ods_fd_vb.ods_fd_user_check_in_log ul
left join ods_fd_vb.ods_fd_users u on ul.user_id = u.user_id
left join dim.dim_fd_region r on u.country = r.region_id
where date(TO_UTC_TIMESTAMP(ul.time, 'America/Los_Angeles')) = '${pt}'
and ul.project is not null and ul.project !='' and r.region_code is not null and r.region_code != ''

union
select
t1.project,
t1.platform_type,
t1.country_code,
null as all_domain_userid,
null,
null,null,null,null,null,null,null,null,
null as checkin_points_domain_userid,
null as checkin_userid,
t1.user_id as checkin_userid_first,
null,null,null,null,
null,null,null,null,null,null,
null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
null,null,null,null,null,null,
null,null,null,null
from (
select
ul.user_id,
ul.project,
case when ul.type = 'android_ap' then 'android_app'
     when ul.type = 'ios_app' then 'ios_app' end as platform_type,
r.region_code as country_code,
date(TO_UTC_TIMESTAMP(ul.time, 'America/Los_Angeles')) as check_date,
Row_Number() OVER (partition by ul.user_id ORDER BY ul.time asc) rank
from ods_fd_vb.ods_fd_user_check_in_log ul
left join ods_fd_vb.ods_fd_users u on ul.user_id = u.user_id
left join dim.dim_fd_region r on u.country = r.region_id
) t1 where t1.rank = 1 and t1.check_date = '${pt}' and t1.country_code !='' and t1.country_code is not null;
