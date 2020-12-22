#!/bin/sh
home=`dirname "$0"`
cd $home

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

hive -f ${shell_path}/dwd_fd_order_goods/create_table.hql

#脚本路径
sql="
INSERT overwrite table dwd.dwd_fd_order_goods
select
/*+ REPARTITION(5) */
ud.sp_duid,
og.rec_id,
og.order_id,
og.goods_style_id,
og.sku,
og.sku_id,
og.goods_id,
og.goods_name,
og.goods_sn,
og.goods_sku,
og.goods_number,
og.market_price,
og.shop_price,
og.shop_price_exchange,
og.shop_price_amount_exchange,
og.coupon_goods_id,
og.goods_price_original,
oi.party_id,
oi.order_sn,
oi.user_id,
oi.order_time_original,
oi.order_time,
oi.order_status,
oi.shipping_status,
oi.pay_status,
oi.consignee,
oi.gender,
oi.country_id,
oi.email,
oi.goods_amount,
oi.goods_amount_exchange,
oi.shipping_fee,
oi.integral,
oi.integral_money,
oi.bonus,
oi.bonus_exchange,
oi.order_amount,
oi.base_currency_id,
oi.order_currency_id,
cy.currency AS order_currency_code,
oi.order_currency_symbol,
oi.order_amount_exchange,
oi.pay_time_original,
oi.pay_time,
oi.coupon_code,
oi.ga_track_id,
oi.taobao_order_sn,
oi.distribution_purchase_order_sn,
oi.language_id,
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
g.virtual_goods_id,
g.cp_goods_id,
g.brand_id,
g.is_complete,
g.is_new,
g.cat_id,
g.cat_name,
g.first_cat_id,
g.first_cat_name,
g.goods_weight
from (
select
rec_id,
order_id,
goods_style_id,
sku,
sku_id,
goods_id,
goods_name,
goods_sn,
goods_sku,
goods_number,
market_price,
shop_price,
shop_price_exchange,
shop_price_amount_exchange,
bonus,
goods_attr,
send_number,
is_real,
extension_code,
parent_id,
is_gift,
goods_status,
coupon_goods_id,
coupon_config_value,
coupon_config_coupon_type,
goods_price_original
from ods_fd_vb.ods_fd_order_goods
)og
LEFT JOIN (
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
email,
goods_amount,
goods_amount_exchange,
shipping_fee,
integral,
integral_money,
bonus,
bonus_exchange,
order_amount,
base_currency_id,
order_currency_id,
order_currency_symbol,
order_amount_exchange,
pay_time as pay_time_original,
cast(unix_timestamp(to_utc_timestamp(pay_time, 'America/Los_Angeles'), 'yyyy-MM-dd HH:mm:ss') as BIGINT) AS pay_time,
coupon_code,
ga_track_id,
additional_amount,
taobao_order_sn,
distribution_purchase_order_sn,
facility_id,
language_id,
coupon_config_value,
coupon_config_coupon_type,
from_domain,
project_name,
user_agent_id
from ods_fd_vb.ods_fd_order_info
where email not regexp 'tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com'
) oi ON og.order_id = oi.order_id
left join (
select du.user_id,du.sp_duid
from (
select user_id, sp_duid,row_number () OVER (PARTITION BY user_id ORDER BY last_update_time DESC) AS rank
from ods_fd_vb.ods_fd_user_duid
where sp_duid IS NOT NULL
)du where du.rank = 1
) ud ON oi.user_id = ud.user_id
LEFT JOIN  dim.dim_fd_user_agent ua ON oi.user_agent_id = ua.user_agent_id
LEFT JOIN  dim.dim_fd_region r ON oi.country_id = r.region_id
LEFT JOIN  dim.dim_fd_language l ON oi.language_id = l.language_id
LEFT JOIN  dim.dim_fd_currency cy ON oi.order_currency_id = cy.currency_id
LEFT JOIN  dim.dim_fd_goods g ON (og.goods_id = g.goods_id and lower(oi.project_name) = lower(g.project_name));
"

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.app.name=fd_dwd_order_goods_gaohaitao"   --conf "spark.sql.output.coalesceNum=40" --conf "spark.dynamicAllocation.minExecutors=40" --conf "spark.dynamicAllocation.initialExecutors=60" -e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi
