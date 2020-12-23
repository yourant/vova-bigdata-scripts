#!/bin/sh
## 脚本参数注释:
## $1 日期%Y-%m-%d【非必传】

if [ ! -n "$1" ] ;then
    pt=`date -d "-1 days" +%Y-%m-%d`
else
    echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $1 +%Y-%m-%d > /dev/null
    if [[ $? -ne 0 ]]; then
        echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
        exit 1
    fi
    pt=$1

fi

#hive sql中使用的变量
echo $pt

#脚本路径
shell_path="/mnt/vova-bigdata-scripts/fd/dwd"

#创建表
hive -f ${shell_path}/dwd_fd_order_info/create_table.hql

sql="
INSERT overwrite table dwd.dwd_fd_order_info
select
/*+ REPARTITION(5) */
ud.sp_duid,
oi.order_id,
oi.party_id,
oi.order_sn,
oi.user_id,
oi.order_time_original,
oi.order_time,
oi.order_status,
oi.shipping_status,
oi.pay_status,
oi.country_id,
oi.mobile,
oi.email,
oi.payment_id,
oi.payment_name,
oi.goods_amount,
oi.goods_amount_exchange,
oi.shipping_fee,
oi.shipping_fee_exchange,
oi.integral,
oi.integral_money,
oi.bonus,
oi.bonus_exchange,
oi.order_amount,
oi.base_currency_id,
oi.order_currency_id,
oi.order_currency_symbol,
oi.rate,
oi.order_amount_exchange,
oi.from_ad,
oi.referer,
oi.pay_time_original,
oi.pay_time,
oi.coupon_code,
oi.order_type_id,
oi.taobao_order_sn,
oi.language_id,
oi.coupon_cat_id,
oi.coupon_config_value,
oi.coupon_config_coupon_type,
oi.is_conversion,
oi.from_domain,
oi.project_name,
oi.user_agent_id,
if(ua.platform_type is null, 'other', ua.platform_type) AS platform_type,
ua.version,
ua.is_app,
ua.device_type,
ua.os_type,
r.region_code AS country_code,
l.language_code AS language_code,
cy.currency AS order_currency_code
from(
select
order_id,
party_id,
order_sn,
user_id,
order_time as order_time_original,
cast(unix_timestamp(to_utc_timestamp(order_time, 'America/Los_Angeles'), 'yyyy-MM-dd HH:mm:ss') as BIGINT) AS order_time,
order_status,
shipping_status,
pay_status,
consignee,
gender,
country as country_id,
province,
province_text,
city,
city_text,
district,
district_text,
address,
zipcode,
tel,
mobile,
email,
best_time,
sign_building,
postscript,
cast(unix_timestamp(to_utc_timestamp(important_day, 'America/Los_Angeles'), 'yyyy-MM-dd') as BIGINT) AS important_day,
sm_id,
shipping_id,
shipping_name,
payment_id,
payment_name,
how_oos,
how_surplus,
pack_name,
card_name,
card_message,
inv_payee,
inv_content,
inv_address,
inv_zipcode,
inv_phone,
goods_amount,
goods_amount_exchange,
shipping_fee,
duty_fee,
shipping_fee_exchange,
duty_fee_exchange,
insure_fee,
shipping_proxy_fee,
payment_fee,
pack_fee,
card_fee,
money_paid,
surplus,
integral,
integral_money,
bonus,
bonus_exchange,
order_amount,
base_currency_id,
order_currency_id,
order_currency_symbol,
rate,
order_amount_exchange,
from_ad,
referer,
confirm_time,
pay_time as pay_time_original,
cast(unix_timestamp(to_utc_timestamp(pay_time, 'America/Los_Angeles'), 'yyyy-MM-dd HH:mm:ss') as BIGINT) AS pay_time,
shipping_time,
cast(unix_timestamp(to_utc_timestamp(shipping_date_estimate, 'America/Los_Angeles'), 'yyyy-MM-dd') as BIGINT) AS shipping_date_estimate,
shipping_carrier,
shipping_tracking_number,
pack_id,
card_id,
coupon_code,
invoice_no,
extension_code,
extension_id,
to_buyer,
pay_note,
invoice_status,
carrier_bill_id,
receiving_time,
biaoju_store_id,
parent_order_id,
track_id,
ga_track_id,
real_paid,
real_shipping_fee,
is_shipping_fee_clear,
is_order_amount_clear,
is_ship_emailed,
proxy_amount,
pay_method,
is_back,
is_finance_clear,
finance_clear_type,
handle_time,
start_shipping_time,
end_shipping_time,
shortage_status,
is_shortage_await,
order_type_id,
special_type_id,
is_display,
misc_fee,
distributor_id,
taobao_order_sn,
distribution_purchase_order_sn,
need_invoice,
facility_id,
language_id,
coupon_cat_id,
coupon_config_value,
coupon_config_coupon_type,
is_conversion,
from_domain,
project_name,
user_agent_id,
display_currency_id,
display_currency_rate,
display_shipping_fee_exchange,
display_duty_fee_exchange,
display_order_amount_exchange,
display_goods_amount_exchange,
display_bonus_exchange,
token,
payer_id
from ods_fd_vb.ods_fd_order_info
where email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
)oi
left join (
select du.user_id,du.sp_duid
from (
select user_id, sp_duid,row_number () OVER (PARTITION BY user_id ORDER BY last_update_time DESC) AS rank
from ods_fd_vb.ods_fd_user_duid
where sp_duid IS NOT NULL
)du where du.rank = 1
) ud ON oi.user_id = ud.user_id
left join dim.dim_fd_user_agent ua ON oi.user_agent_id = ua.user_agent_id
left join dim.dim_fd_region r ON oi.country_id = r.region_id
left join dim.dim_fd_language l ON oi.language_id = l.language_id
left join dim.dim_fd_currency cy ON oi.order_currency_id = cy.currency_id;

"

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.app.name=fd_dwd_order_info_gaohaitao"   --conf "spark.sql.output.coalesceNum=40" --conf "spark.dynamicAllocation.minExecutors=40" --conf "spark.dynamicAllocation.initialExecutors=60" -e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi
