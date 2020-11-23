#!/bin/sh
home=`dirname "$0"`
cd $home

if [ ! -n "$1" ] ;then
    pt=`date -d "-1 days" +%Y-%m-%d`
    pt_last=`date -d "-2 days" +%Y-%m-%d`
else
    echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $1 +%Y-%m-%d > /dev/null
    if [[ $? -ne 0 ]]; then
        echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
        exit
    fi
    pt=$1
    pt_last=`date -d "$1 -1 days" +%Y-%m-%d`

fi

#hive sql中使用的变量
echo $pt
echo $pt_last

#app用户优惠券使用情况
sql="
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
INSERT overwrite table dwb.dwb_fd_app_user_coupon_order PARTITION (pt)
select
t1.project_name as project_name,
COALESCE(t1.platform_type,t2.platform_type,'unknown') as platform_type,
COALESCE(t1.country_code,t2.country_code,'unknown') as country_code,
t1.coupon_config_id,
t1.coupon_code as coupon_give,
if(t3.coupon_code is not null,t1.coupon_code,null) as coupon_used,
if(t3.coupon_code is not null and t3.pay_status = 2,t1.coupon_code,null) as coupon_used_success,
if(t3.coupon_code is not null and (unix_timestamp(t3.order_time) - unix_timestamp(t1.coupon_give_date))/3600 > 0 and (unix_timestamp(t3.order_time) - unix_timestamp(t1.coupon_give_date))/3600 < 1,t1.coupon_code,null) as coupon_used_1h,
if(t3.coupon_code is not null and (unix_timestamp(t3.order_time) - unix_timestamp(t1.coupon_give_date))/3600 >= 1 and (unix_timestamp(t3.order_time) - unix_timestamp(t1.coupon_give_date))/3600 < 24,t1.coupon_code,null) as coupon_used_24h,
if(t3.coupon_code is not null and (unix_timestamp(t3.order_time) - unix_timestamp(t1.coupon_give_date))/3600 >= 24 and (unix_timestamp(t3.order_time) - unix_timestamp(t1.coupon_give_date))/3600 < 48,t1.coupon_code,null) as coupon_used_48h,
if(t3.coupon_code is not null and (unix_timestamp(t3.order_time) - unix_timestamp(t1.coupon_give_date))/3600 >= 48 and (unix_timestamp(t3.order_time) - unix_timestamp(t1.coupon_give_date))/3600 < 72,t1.coupon_code,null) as coupon_used_72h,
if(t3.coupon_code is not null and (unix_timestamp(t3.order_time) - unix_timestamp(t1.coupon_give_date))/3600 >= 72,t1.coupon_code,null) as coupon_used_greater_72h,
t1.pt as pt
from (
select
tab1.user_id as user_id,
tab1.coupon_code as coupon_code,
cast(tab1.coupon_config_id as string) as coupon_config_id,
tab1.coupon_config_comment as coupon_config_comment,
tab1.coupon_gtime as coupon_gtime,
tab1.coupon_give_date as coupon_give_date,
COALESCE(tab2.project_name,tab3.reg_site_name) as project_name,
COALESCE(tab2.platform,null) as platform_type,
COALESCE(tab2.country_code,null) as country_code,
date(tab1.coupon_give_date) as pt
from (
select
oc.user_id,
oc.coupon_code,
oc.coupon_config_id,
kcc.coupon_config_comment,
oc.coupon_gtime,
oc.coupon_give_date
from (
select
user_id,
coupon_code,
coupon_config_id,
coupon_gtime,
from_unixtime(coupon_gtime, 'yyyy-MM-dd HH:mm:ss') as coupon_give_date
from ods_fd_vb.ods_fd_ok_coupon
where can_use_times = 1
and length(coupon_code) = 16
and date(from_unixtime(coupon_gtime, 'yyyy-MM-dd HH:mm:ss')) >= date_sub('$pt',10)
and date(from_unixtime(coupon_gtime, 'yyyy-MM-dd HH:mm:ss')) <= '$pt'
) oc
left join (select coupon_config_id,coupon_config_comment from ods_fd_vb.ods_fd_ok_coupon_config ) kcc ON oc.coupon_config_id = kcc.coupon_config_id

)tab1
left join(

select
t1.user_id,t1.project_name,t1.platform,t1.country_code
from (
select
distinct
user_id,
project_name,
case
when platform = 'ios' then 'ios_app'
when platform = 'android' then 'android_app'
else 'unknown'
end as platform,
country_code,
Row_Number() OVER (partition by user_id  ORDER BY event_time desc) rank
from ods_fd_vb.ods_fd_app_install_record
where  user_id is not null and user_id != 0
) t1 where t1.rank = 1

) tab2 on tab2.user_id = tab1.user_id
left join (select user_id,reg_site_name from ods_fd_vb.ods_fd_users ) tab3 on tab3.user_id = tab1.user_id

) t1
left join (

select user_id,project_name,country_code,platform_type
from (
select
user_id,
project_name,
country_code,
case
when is_app = 1 and os_type = 'ios' then 'ios_app'
when is_app = 1 and os_type = 'android' then 'android_app'
else 'unknown'
end as platform_type,
Row_Number() OVER (partition by user_id,project_name ORDER BY order_time desc) rank
from dwd.dwd_fd_order_info
where user_id is not null and user_id != 0
)t0 WHERE t0.rank = 1

) t2 on (t1.user_id = t2.user_id and t1.project_name = t2.project_name)
left join (
select
user_id,
from_unixtime(pay_time,'yyyy-MM-dd hh:mm:ss') as order_time,
coupon_code,
project_name,
pay_status
from dwd.dwd_fd_order_info

)t3 on t3.coupon_code = t1.coupon_code;

"

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.app.name=fd_dwd_app_user_coupon_gaohaitao"   --conf "spark.sql.output.coalesceNum=30" --conf "spark.dynamicAllocation.minExecutors=30" --conf "spark.dynamicAllocation.initialExecutors=50" -e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi
echo "step1: dwb_fd_app_user_coupon_gaohaitao table is finished !"

