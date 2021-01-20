insert overwrite table dwd.dwd_fd_app_points_page partition (pt='${pt}')
select
      /*+ REPARTITION(1) */
      project,
      platform_type,
      country as country_code,
      domain_userid as all_domain_userid,
      session_id as all_session,
      if(page_code = 'myrewards',domain_userid,null) as checkin_points_domain_userid,
      if(page_code = 'big_wheel',domain_userid,null) as play_visit_domain_userid,
      if(user_id != '0' and user_id is not null and user_id != '',domain_userid,null) as user_login_domain_userid,
      if(page_code in('myrewards'),domain_userid,null) as points_domain_userid,
      if(page_code in('myrewards') and referrer_url in('homepage'),domain_userid,null) as points_homepage_domain_userid,
      if(page_code in('myrewards') and referrer_url in('userZone'),domain_userid,null) as points_userzone_domain_userid,
      if(page_code in('myrewards') and referrer_url in('account'),domain_userid,null) as points_account_domain_userid,
      if(page_code in('myrewards') and referrer_url like 'afterPay/order_sn%',domain_userid,null) as points_afterpay_domain_userid,
      if(page_code in('myrewards') and referrer_url not in ('homepage','userZone','account','afterPay'),domain_userid,null) as points_others_domain_userid
from ods_fd_snowplow.ods_fd_snowplow_all_event
where pt = '${pt}' and platform_type in ('android_app','ios_app')
and project is not null and project !='' and country is not null and country != '';
