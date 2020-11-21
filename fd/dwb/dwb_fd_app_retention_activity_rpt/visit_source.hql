insert overwrite table dwb.dwb_fd_app_retention_activity_rpt partition (pt = '${pt}',classify='visit_source')
select
project,
platform_type,
country as country_code,
null,null,
null,null,null,null,null,null,null,null,
null,null,null,
null,null,null,null,
null,null,null,null,null,null,
null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
if(page_code = 'myrewards',domain_userid,null) as myrewards_domain_userid,
if(page_code = 'myrewards' and referrer_url = 'homepage',domain_userid,null) as homepage_domain_userid,
if(page_code = 'myrewards' and referrer_url = 'userZone',domain_userid,null) as userZone_domain_userid,
if(page_code = 'myrewards' and referrer_url = 'account',domain_userid,null) as account_domain_userid,
if(page_code = 'myrewards' and referrer_url like 'afterPay/order_sn%',domain_userid,null) as afterPay_domain_userid,
if(page_code = 'myrewards' and referrer_url not in('homepage','userZone','account','afterPay'),domain_userid,null) as other_domain_userid,
null,null,null,null
from ods_fd_snowplow.ods_fd_snowplow_all_event
where pt = '${pt}' and platform_type in ('android_app','ios_app') and page_code = 'myrewards';
