insert overwrite table dwb.dwb_fd_app_retention_activity_rpt partition (pt='2020-10-30')
select
/*+ REPARTITION(10) */
tab1.project as project,
tab1.platform_type as platform_type,
tab1.country_code as country_code,
tab5.retention_domain_userid_1d as retention_domain_userid_1d_cnt,
tab5.retention_domain_userid_2d as retention_domain_userid_2d_cnt,
tab5.login_domain_userid_1d as login_domain_userid_1d_cnt,
tab5.login_domain_userid_2d as login_domain_userid_2d_cnt,
tab5.checkin_domain_userid_1d as checkin_domain_userid_1d_cnt,
tab5.checkin_domain_userid_2d as checkin_domain_userid_2d_cnt,
tab5.play_domain_userid_1d as play_domain_userid_1d_cnt,
tab5.play_domain_userid_2d as play_domain_userid_2d_cnt,
tab1.checkin_points_domain_userid as checkin_points_domain_userid_cnt,
tab2.checkin_userid as checkin_userid_cnt,
tab2.checkin_userid_first as checkin_userid_first_cnt,
tab1.all_domain_userid as all_domain_userid_cnt,
tab1.play_visit_domain_userid as play_visit_domain_userid_cnt,
tab3.play_join_userid as play_join_userid_cnt,
tab3.play_first_join_userid as play_first_join_userid_cnt,
tab3.play_points_join_userid as play_points_join_userid_cnt,
tab1.all_session as all_session_cnt,
tab1.user_login_domain_userid as user_login_domain_userid_cnt,
tab4.user_register_domain_userid as user_register_domain_userid_cnt,
tab4.user_new_domain_userid as user_new_domain_userid_cnt,
tab4.user_new_register_domain_userid as user_new_register_domain_userid_cnt,
tab4.user_order_id as user_order_id_cnt,
tab4.user_new_order_id as user_new_order_id_cnt,
tab2.all_check_user_id as all_check_user_id_cnt,
tab2.all_cont_check_user_id as all_cont_check_user_id_cnt,
tab2.acc_check_user_id_1th as acc_check_user_id_1th_cnt,
tab2.acc_check_user_id_2th as acc_check_user_id_2th_cnt,
tab2.acc_check_user_id_3th as acc_check_user_id_3th_cnt,
tab2.acc_check_user_id_4th as acc_check_user_id_4th_cnt,
tab2.acc_check_user_id_5th as acc_check_user_id_5th_cnt,
tab2.acc_check_user_id_6th as acc_check_user_id_6th_cnt,
tab2.acc_check_user_id_7th as acc_check_user_id_7th_cnt,
tab2.acc_check_user_id_greater_7th as acc_check_user_id_greater_7th_cnt,
tab2.cont_check_user_id_2th as cont_check_user_id_2th_cnt,
tab2.cont_check_user_id_3th as cont_check_user_id_3th_cnt,
tab2.cont_check_user_id_4th as cont_check_user_id_4th_cnt,
tab2.cont_check_user_id_5th as cont_check_user_id_5th_cnt,
tab2.cont_check_user_id_6th as cont_check_user_id_6th_cnt,
tab2.cont_check_user_id_7th as cont_check_user_id_7th_cnt,
tab2.cont_check_user_id_greater_7th as cont_check_user_id_greater_7th_cnt,
tab1.points_domain_userid as points_domain_userid_cnt,
tab1.points_homepage_domain_userid as points_homepage_domain_userid_cnt,
tab1.points_userzone_domain_userid as points_userzone_domain_userid_cnt,
tab1.points_account_domain_userid as points_account_domain_userid_cnt,
tab1.points_afterpay_domain_userid as points_afterpay_domain_userid_cnt,
tab1.points_others_domain_userid as points_others_domain_userid_cnt,
tab4.user_new_first_order_id as user_new_first_order_id_cnt,
tab4.user_new_first_coupon_order_id as user_new_first_coupon_order_id_cnt,
tab4.user_new_first_success_order_id as user_new_first_success_order_id_cnt,
tab4.user_new_first_success_coupon_order_id as user_new_first_success_coupon_order_id_cnt
from (

	select
		nvl(project,'all') as project,
		nvl(platform_type,'all') as platform_type,
		nvl(country_code,'all') as country_code,
		count(distinct all_domain_userid) as all_domain_userid,
		count(distinct all_session) as all_session,
		count(distinct checkin_points_domain_userid) as checkin_points_domain_userid,
		count(distinct play_visit_domain_userid) as play_visit_domain_userid,
		count(distinct user_login_domain_userid) as user_login_domain_userid,
		count(points_domain_userid) as points_domain_userid,
		count(points_homepage_domain_userid) as points_homepage_domain_userid,
		count(points_userzone_domain_userid) as points_userzone_domain_userid,
		count(points_account_domain_userid) as points_account_domain_userid,
		count(points_afterpay_domain_userid) as points_afterpay_domain_userid,
		count(points_others_domain_userid) as points_others_domain_userid
	from dwd.dwd_fd_app_points_page
	where pt = '2021-01-11'
	and length(project) > 1
	and length(country_code) = 2
	and length(platform_type) > 0
	group by project,platform_type,country_code with cube

) tab1
left join(

	select
		nvl(project,'all') as project,
	    nvl(platform_type,'all') as platform_type,
	    nvl(country_code,'all') as country_code,

	    count(distinct checkin_userid) as checkin_userid,
	    count(distinct checkin_userid_first) as checkin_userid_first,
	    count(distinct all_check_user_id) as all_check_user_id,
	    count(distinct all_cont_check_user_id) as all_cont_check_user_id,
	    count(distinct acc_check_user_id_1th) as acc_check_user_id_1th,
	    count(distinct acc_check_user_id_2th) as acc_check_user_id_2th,
	    count(distinct acc_check_user_id_3th) as acc_check_user_id_3th,
	    count(distinct acc_check_user_id_4th) as acc_check_user_id_4th,
	    count(distinct acc_check_user_id_5th) as acc_check_user_id_5th,
	    count(distinct acc_check_user_id_6th) as acc_check_user_id_6th,
	    count(distinct acc_check_user_id_7th) as acc_check_user_id_7th,
	    count(distinct acc_check_user_id_greater_7th) as acc_check_user_id_greater_7th,
	    count(distinct cont_check_user_id_2th) as cont_check_user_id_2th,
	    count(distinct cont_check_user_id_3th) as cont_check_user_id_3th,
	    count(distinct cont_check_user_id_4th) as cont_check_user_id_4th,
	    count(distinct cont_check_user_id_5th) as cont_check_user_id_5th,
	    count(distinct cont_check_user_id_6th) as cont_check_user_id_6th,
	    count(distinct cont_check_user_id_7th) as cont_check_user_id_7th,
	    count(distinct cont_check_user_id_greater_7th) as cont_check_user_id_greater_7th
	from dwd.dwd_fd_app_checkin
	where pt = '2021-01-11'
	and length(project) > 1
	and length(country_code) = 2
	and length(platform_type) > 0
	group by project,platform_type,country_code with cube

)tab2 on tab2.project = tab1.project
	 and tab2.platform_type = tab1.platform_type
	 and tab2.country_code = tab1.country_code

left join (

	select
		nvl(project,'all') as project,
		nvl(platform_type,'all') as platform_type,
		nvl(country_code,'all') as country_code,
		count(distinct play_join_userid) as play_join_userid,
		count(distinct play_first_join_userid) as play_first_join_userid,
		count(distinct play_points_join_userid) as play_points_join_userid
	from dwd.dwd_fd_app_play_wheel
	where pt = '2021-01-11'
	and length(project) > 1
	and length(country_code) = 2
	and length(platform_type) > 0
	group by project,platform_type,country_code with cube

)tab3 on tab3.project = tab1.project
	 and tab3.platform_type = tab1.platform_type
	 and tab3.country_code = tab1.country_code

left join (

	select
		nvl(project,'all') as project,
	    nvl(platform_type,'all') as platform_type,
	    nvl(country_code,'all') as country_code,
	    count(distinct user_register_domain_userid) as user_register_domain_userid,
	    count(distinct user_new_domain_userid) as user_new_domain_userid,
	    count(distinct user_new_register_domain_userid) as user_new_register_domain_userid,
	    count(distinct user_order_id) as user_order_id,
	    count(distinct user_new_order_id) as user_new_order_id,
	    count(distinct user_new_first_order_id) as user_new_first_order_id,
	    count(distinct user_new_first_coupon_order_id) as user_new_first_coupon_order_id,
	    count(distinct user_new_first_success_order_id) as user_new_first_success_order_id,
	    count(distinct user_new_first_success_coupon_order_id) as user_new_first_success_coupon_order_id
	from dwd.dwd_fd_app_register
	where pt = '2021-01-11'
	and length(project) > 1
	and length(country_code) = 2
	and length(platform_type) > 0
	group by project,platform_type,country_code with cube

)tab4 on tab4.project = tab1.project
	 and tab4.platform_type = tab1.platform_type
	 and tab4.country_code = tab1.country_code

left join (

	select
		nvl(project,'all') as project,
		nvl(platform_type,'all') as platform_type,
		nvl(country_code,'all') as country_code,
		 count(distinct retention_domain_userid_2d) as retention_domain_userid_2d,
		 count(distinct retention_domain_userid_1d) as retention_domain_userid_1d,
		 count(distinct login_domain_userid_2d) as login_domain_userid_2d,
		 count(distinct login_domain_userid_1d) as login_domain_userid_1d,
		 count(distinct checkin_domain_userid_2d) as checkin_domain_userid_2d,
		 count(distinct checkin_domain_userid_1d) as checkin_domain_userid_1d,
		 count(distinct play_domain_userid_2d) as play_domain_userid_2d,
		 count(distinct play_domain_userid_1d) as play_domain_userid_1d
	from dwd.dwd_fd_app_user_retention
	where pt = '2021-01-11'
	and length(project) > 1
	and length(country_code) = 2
	and length(platform_type) > 0
	group by project,platform_type,country_code with cube

)tab5 on tab5.project = tab1.project
	 and tab5.platform_type = tab1.platform_type
	 and tab5.country_code = tab1.country_code;