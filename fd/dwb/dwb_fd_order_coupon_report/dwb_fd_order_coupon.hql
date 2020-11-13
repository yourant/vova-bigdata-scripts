CREATE TABLE IF NOT EXISTS dwb.dwb_fd_order_coupon
(
    project_name     string comment '组织',
    coupon_code      string COMMENT '优惠券code',
    order_id 	     BIGINT COMMENT '订单id',
    order_sn         string COMMENT '订单SN',
    order_amount     DECIMAL(10, 2) COMMENT '订单金额',
    gmv              DECIMAL(10, 2) COMMENT 'GMV',
    shipping_fee     DECIMAL(10, 2) COMMENT '运费',
    true_bonus       DECIMAL(10, 2) COMMENT '优惠金额',
    order_time_utc   string comment '订单utc时间',
    order_time_pst   string COMMENT '订单洛杉矶时间',
    order_status     int COMMENT '订单状态',
    pay_status       int COMMENT '订单支付状态',
    platform_type    string comment '平台',
    country_code     string COMMENT '国家code',
    language_code    string COMMENT '语言code'
) COMMENT '订单优惠券信息表'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table dwb.dwb_fd_order_coupon PARTITION (dt)
select 
	project_name,
	coupon_code,
	order_id,
	order_sn, 
	order_amount as order_amount,
	(order_amount - shipping_fee) as gmv,
	shipping_fee as shipping_fee,
	-bonus as true_bonus,
	from_unixtime(order_time,'yyyy-MM-dd hh:mm:ss') as order_time_utc,/*utc时间*/
	from_utc_timestamp(from_unixtime(order_time,'yyyy-MM-dd hh:mm:ss'),'PST') as order_time_pst, /* 洛杉矶时间*/
	order_status,
	pay_status,
	platform_type,
	country_code,
	language_code,
	date(from_unixtime(order_time,'yyyy-MM-dd hh:mm:ss')) as dt
from dwd.dwd_fd_order_info 
where 
coupon_code !='' 
and email not like '%@tetx.com%'
and email not like '%@i9i8.com%'
and email not like '%@qq.com%'
and email not like '%@163.com%'
and email not like '%@jjshouse.com%'
and email not like '%@jenjenhouse.com%'
and dt = '${hiveconf:dt}'
and date(from_unixtime(order_time,'yyyy-MM-dd hh:mm:ss')) >= '2020-07-01';
