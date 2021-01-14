insert overwrite table dwb.dwb_fd_app_retention_activity_rpt partition (pt='${pt}')
select
/*+ REPARTITION(10) */
nvl(project,'all') as project,
nvl(platform_type,'all') as platform_type,
nvl(country_code,'all') as country_code,
count(distinct retention_domain_userid_1d) as retention_domain_userid_1d_cnt,
count(distinct retention_domain_userid_2d) as retention_domain_userid_2d_cnt,
count(distinct login_domain_userid_1d) as login_domain_userid_1d_cnt,
count(distinct login_domain_userid_2d) as login_domain_userid_2d_cnt,
count(distinct checkin_domain_userid_1d) as checkin_domain_userid_1d_cnt,
count(distinct checkin_domain_userid_2d) as checkin_domain_userid_2d_cnt,
count(distinct play_domain_userid_1d) as play_domain_userid_1d_cnt,
count(distinct play_domain_userid_2d) as play_domain_userid_2d_cnt,
count(distinct checkin_points_domain_userid) as checkin_points_domain_userid_cnt,
count(distinct checkin_userid) as checkin_userid_cnt,
count(distinct checkin_userid_first) as checkin_userid_first_cnt,
count(distinct all_domain_userid) as all_domain_userid_cnt,
count(distinct play_visit_domain_userid) as play_visit_domain_userid_cnt,
count(distinct play_join_userid) as play_join_userid_cnt,
count(distinct play_first_join_userid) as play_first_join_userid_cnt,
count(distinct play_points_join_userid) as play_points_join_userid_cnt,
count(distinct all_session) as all_session_cnt,
count(distinct user_login_domain_userid) as user_login_domain_userid_cnt,
count(distinct user_register_domain_userid) as user_register_domain_userid_cnt,
count(distinct user_new_domain_userid) as user_new_domain_userid_cnt,
count(distinct user_new_register_domain_userid) as user_new_register_domain_userid_cnt,
count(distinct user_order_id) as user_order_id_cnt,
count(distinct user_new_order_id) as user_new_order_id_cnt,
count(distinct all_check_user_id) as all_check_user_id_cnt,
count(distinct all_cont_check_user_id) as all_cont_check_user_id_cnt,
count(distinct acc_check_user_id_1th) as acc_check_user_id_1th_cnt,
count(distinct acc_check_user_id_2th) as acc_check_user_id_2th_cnt,
count(distinct acc_check_user_id_3th) as acc_check_user_id_3th_cnt,
count(distinct acc_check_user_id_4th) as acc_check_user_id_4th_cnt,
count(distinct acc_check_user_id_5th) as acc_check_user_id_5th_cnt,
count(distinct acc_check_user_id_6th) as acc_check_user_id_6th_cnt,
count(distinct acc_check_user_id_7th) as acc_check_user_id_7th_cnt,
count(distinct acc_check_user_id_greater_7th) as acc_check_user_id_greater_7th_cnt,
count(distinct cont_check_user_id_2th) as cont_check_user_id_2th_cnt,
count(distinct cont_check_user_id_3th) as cont_check_user_id_3th_cnt,
count(distinct cont_check_user_id_4th) as cont_check_user_id_4th_cnt,
count(distinct cont_check_user_id_5th) as cont_check_user_id_5th_cnt,
count(distinct cont_check_user_id_6th) as cont_check_user_id_6th_cnt,
count(distinct cont_check_user_id_7th) as cont_check_user_id_7th_cnt,
count(distinct cont_check_user_id_greater_7th) as cont_check_user_id_greater_7th_cnt,
count(points_domain_userid) as points_domain_userid_cnt,
count(points_homepage_domain_userid) as points_homepage_domain_userid_cnt,
count(points_userzone_domain_userid) as points_userzone_domain_userid_cnt,
count(points_account_domain_userid) as points_account_domain_userid_cnt,
count(points_afterpay_domain_userid) as points_afterpay_domain_userid_cnt,
count(points_others_domain_userid) as points_others_domain_userid_cnt,
count(distinct user_new_first_order_id) as user_new_first_order_id_cnt,
count(distinct user_new_first_coupon_order_id) as user_new_first_coupon_order_id_cnt,
count(distinct user_new_first_success_order_id) as user_new_first_success_order_id_cnt,
count(distinct user_new_first_success_coupon_order_id) as user_new_first_success_coupon_order_id_cnt
from dwd.dwd_fd_app_retention_activity
where pt = '${pt}' and country_code !='' and country_code is not null
group by project,platform_type,country_code with cube;