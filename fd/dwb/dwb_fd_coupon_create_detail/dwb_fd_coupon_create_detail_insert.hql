set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwb.dwb_fd_coupon_create_detail PARTITION (pt)
select
	/*+ REPARTITION(1) */
	project_name,
	coupon_type_name,
	coupon_create_cnt,
	coupon_gived_cnt,
	coupon_use_fail_cnt,
	coupon_use_success_cnt,
	coupon_used_total_cnt,
	pt
from(
	select

		nvl(project_name,'all') as project_name,
		nvl(coupon_type_name,'all') as coupon_type_name,
		count(distinct coupon_code) as coupon_create_cnt,--当天创建量
		sum(can_use_times) as coupon_gived_cnt,--红包发放总量
		nvl(sum(coupon_use_fail),0) as coupon_use_fail_cnt,--已使用未付款量
		nvl(sum(coupon_use_success),0) as coupon_use_success_cnt,--已使用已付款量
		nvl(sum(coupon_use_fail + coupon_use_success),0) as coupon_used_total_cnt,--使用总量
		nvl(pt,'all') as pt
	from(
		select

			nvl(oii.project_name,'NA') as project_name, --组织
			oc.coupon_code,--红包code
		    oc.can_use_times, --红包可以使用次数
		    nvl(occ.coupon_type_name,'NA') as coupon_type_name,--红包类型名
		    oii.coupon_use_success, --使用并且付款成功的红包数量
		    oii.coupon_use_fail, --使用并且不是付款成功的红包数量
		    oc.coupon_ctime_date as pt --红包创建时间PRC
		from (

			select
			    coupon_code,
			    date_format(from_utc_timestamp(from_unixtime(coupon_ctime, 'yyyy-MM-dd HH:mm:ss'), 'PRC'), 'yyyy-MM-dd') as coupon_ctime_date, --红包创建时间PRC
			    can_use_times --红包可以使用次数
			from ods_fd_vb.ods_fd_ok_coupon
			where date_format(from_utc_timestamp(from_unixtime(coupon_ctime, 'yyyy-MM-dd HH:mm:ss'), 'PRC'), 'yyyy-MM-dd') >= date_sub('${pt}',60)
			and date_format(from_utc_timestamp(from_unixtime(coupon_ctime, 'yyyy-MM-dd HH:mm:ss'), 'PRC'), 'yyyy-MM-dd') <= '${pt}'

		) oc
		left join (

			select
				coupon_code,coupon_type_name,count(1)
			from(
				select
					oi.coupon_code,
					CASE
						WHEN kcc.coupon_config_comment = '10001' THEN 'A-售前折扣红包'
						WHEN kcc.coupon_config_comment = '10002' THEN 'B-售后退款/折扣红包'
						WHEN kcc.coupon_config_comment = '10003' THEN 'C-关税红包'
						WHEN kcc.coupon_config_comment = '10004' THEN 'D-好评红包'
						WHEN kcc.coupon_config_comment = '10005' THEN 'E-survey红包'
						WHEN kcc.coupon_config_comment = '10006' THEN 'F-售中小金额退款红包'
						WHEN kcc.coupon_config_comment = '10011' THEN 'H-注册送红包'
						WHEN kcc.coupon_config_comment = '10007' THEN 'I-newsletter红包'
						WHEN kcc.coupon_config_comment = '10008' THEN 'J-未付款红包'
						WHEN kcc.coupon_config_comment = '10009' THEN 'K-大客户红包'
						WHEN kcc.coupon_config_comment = '10010' THEN 'L-其他'
						WHEN kcc.coupon_config_comment = '10012' THEN 'M-EXTRA5'
						WHEN kcc.coupon_config_comment = '10013' THEN 'N-EXTRA10'
						WHEN kcc.coupon_config_comment = '10014' THEN '0-测试'
						WHEN kcc.coupon_config_comment = '10017' THEN '10017' -- 用户确认收货赠送coupon
						WHEN kcc.coupon_config_comment = '10101' THEN 'fdapp用户连续登陆3天赠送coupon'
						WHEN kcc.coupon_config_comment = '10102' THEN 'fdapp用户连续登陆6天赠送coupon'
						WHEN kcc.coupon_config_comment = '10103' THEN '用户完成首单推送后送coupon'
						WHEN kcc.coupon_config_comment = '10104' THEN 'fdapp注册coupon[3天有效期]'
						WHEN kcc.coupon_config_comment = '10105' THEN 'fdapp注册coupon[7天有效期]'
						WHEN ISNULL(kcc.coupon_config_comment) AND oi.bonus != 0 AND oi.integral != 0 THEN '积分抵扣'
						WHEN ISNULL(kcc.coupon_config_comment) AND oi.bonus != 0 AND oi.email REGEXP "tetx.com|i9i8.com" THEN '手工无红包抵扣'
						WHEN ISNULL(kcc.coupon_config_comment) AND oi.bonus != 0 THEN '未知无红包抵扣'
					ELSE LOWER(kcc.coupon_config_comment) END coupon_type_name

				from dwd.dwd_fd_order_info oi
				left join ods_fd_vb.ods_fd_ok_coupon kc ON oi.coupon_code = kc.coupon_code
				left join ods_fd_vb.ods_fd_ok_coupon_config kcc ON kc.coupon_config_id = kcc.coupon_config_id
				where oi.order_status != 2

			)t group by coupon_code,coupon_type_name

		)occ on oc.coupon_code = occ.coupon_code

		left join (

			select
				project_name,
				coupon_code,
				count(if(pay_status = 2,coupon_code,null)) as coupon_use_success, --使用并且付款成功的红包数量
				count(if(pay_status != 2,coupon_code,null)) as coupon_use_fail --使用并且不是付款成功的红包数量
			from (
				select
				user_id,
				pay_time,
				from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss') as pay_date,--支付时间UTC
				coupon_code,
				project_name,
				pay_status,
				bonus,
				integral,
				email
			from dwd.dwd_fd_order_info
			where order_status != 2
			)tab group by project_name,coupon_code

		)oii on oii.coupon_code = oc.coupon_code
	)tab group by tab.pt,tab.project_name,tab.coupon_type_name with cube
)t where pt !='all';