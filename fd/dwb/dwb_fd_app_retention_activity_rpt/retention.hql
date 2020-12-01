insert overwrite table dwd.dwd_fd_app_retention_activity partition (pt='${pt}',classify='retention')
select
 /*+ REPARTITION(1) */
rewards_day1.project as project,
rewards_day1.platform_type as platform_type,
rewards_day1.country as country_code,
null,null,
rewards_day1.domain_userid as retention_domain_userid_2d,
rewards_today.domain_userid as retention_domain_userid_1d,
if(rewards_day1.user_id != '0' and rewards_day1.user_id is not null and rewards_day1.user_id != '',rewards_day1.domain_userid,null) as login_domain_userid_2d,
if(rewards_day1.user_id != '0' and rewards_day1.user_id is not null and rewards_day1.user_id != '',rewards_today.domain_userid,null) as login_domain_userid_1d,
if(rewards_day1.list_name in ('check_in_success','my_rewards_check_success') and rewards_day1.event_name in('common_impression') and rewards_day1.element_name in('my_rewards_check_success','check_coupon'),rewards_day1.domain_userid,null) as checkin_domain_userid_2d,
if(rewards_day1.list_name in ('check_in_success','my_rewards_check_success') and rewards_day1.event_name in('common_impression') and rewards_day1.element_name in('my_rewards_check_success','check_coupon'),rewards_today.domain_userid,null) as checkin_domain_userid_1d,
if(rewards_day1.element_name in('free_play'),rewards_day1.domain_userid,null) as play_domain_userid_2d,
if(rewards_day1.element_name in('free_play'),rewards_today.domain_userid,null) as play_domain_userid_1d,
null,null,null,
null,null,null,null,
null,null,null,null,null,null,
null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
null,null,null,null,null,null,
null,null,null,null
from(
select distinct user_id,domain_userid,project,country,platform_type,event_name,common_element.element_name as element_name,common_element.list_type as list_name
from (
select user_id,domain_userid,project,country,platform_type,event_name,element_event_struct
from ods_fd_snowplow.ods_fd_snowplow_all_event
where  pt = date_sub('${pt}',1) and platform_type in ('android_app','ios_app')
and project is not null and project !=''
)ce_d6 LATERAL VIEW OUTER explode(ce_d6.element_event_struct)ce_d6 as common_element
) rewards_day1 left join(
select distinct domain_userid,user_id
from ods_fd_snowplow.ods_fd_snowplow_all_event
where pt = '${pt}' and platform_type in ('android_app','ios_app')
) rewards_today on rewards_day1.domain_userid = rewards_today.domain_userid;