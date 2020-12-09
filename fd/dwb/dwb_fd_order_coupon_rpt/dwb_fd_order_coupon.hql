INSERT overwrite table dwb.dwb_fd_order_coupon_rpt PARTITION (pt = '${pt}')
select
/*+ REPARTITION(1) */
	project_name,
	coupon_code,
	order_id,
	order_sn, 
	order_amount as order_amount,
	(order_amount - shipping_fee) as gmv,
	shipping_fee as shipping_fee,
	-bonus as true_bonus,
	from_unixtime(order_time,'yyyy-MM-dd HH:mm:ss') as order_time_utc,
	from_utc_timestamp(from_unixtime(order_time,'yyyy-MM-dd HH:mm:ss'),'PST') as order_time_pst,
	order_status,
	pay_status,
	platform_type,
	country_code,
	language_code
from dwd.dwd_fd_order_info 
where 
coupon_code !='' 
and email NOT REGEXP 'tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com'
and date(from_unixtime(order_time,'yyyy-MM-dd HH:mm:ss')) = '${pt}';
