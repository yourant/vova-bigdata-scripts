#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

#rpt_checkout
##dependence
#ods_vova_vts.ods_vova_order_card_installments_record
#ods_vova_vts.ods_vova_paypal_txn
#dwd.dwd_vova_log_screen_view
#dwd.dwd_vova_log_common_click
#dwd.dwd_vova_log_page_view
#dim.dim_vova_order_goods

sql="
INSERT OVERWRITE TABLE dwb.dwb_vova_checkout PARTITION (pt = '${cur_date}')
SELECT
/*+ REPARTITION(1) */
'${cur_date}' AS action_date,
log_data.region_code,
log_data.platform,
log_data.payment_name,
log_data.dau,
log_data.checkout_new_uv,
log_data.checkout_place_order_uv,
log_data.payment_uv,
log_data.checkout_deliver_to_uv,
log_data.checkout_deliver_to_popular_uv,
log_data.checkout_add_address_uv,
log_data.checkout_address_edit_uv,
log_data.checkout_deliver_address_uv,
log_data.checkout_my_bag_uv,
log_data.checkout_coupon_uv,
log_data.checkout_balance_uv,
log_data.checkout_cod_uv,
log_data.checkout_credit_card_uv,
log_data.payment_new_card_to_pay_uv,
log_data.payment_new_card_to_pay_pv,
log_data.payment_confirm_uv,
log_data.payment_card_uv,
log_data.payment_paypal_uv,
log_data.card_choosing_uv,
log_data.payment_checkout_uv,
log_data.payment_checkout_pv,
log_data.deliver_to_back_uv,
log_data.checkout_new_stay_uv,
log_data.add_new_card_stay_uv,
log_data.add_new_card_leave_uv,
log_data.add_new_card_stay_leave_uv,
log_data.checkout_address_edit_stay_uv,
log_data.checkout_address_edit_leave_uv,
log_data.checkout_address_edit_stay_leave_uv,
log_data.payment_stay_uv,
log_data.payment_leave_uv,
log_data.payment_stay_leave_uv,
log_data.checkout_out_of_stock_uv,
log_data.sku_change_stay_uv,
log_data.deliver_to_checkout_out_of_stock_uv,
log_data.deliver_to_sku_change_stay_uv,
log_data.checkout_sku_change_uv,
log_data.checkout_zero_subtotal_uv,
log_data.checkout_price_change_uv,
log_data.intermediate_uv,
log_data.intermediate_continue_uv,
log_data.intermediate_edit_uv,
log_data.intermediate_back_uv,
log_data.intermediate_invalid_uv,
log_data.intermediate_exception_uv,
log_data.payment_success,
log_data.payment_fail,
order_data.gmv,
order_data.paid_order_cnt,
order_data.paid_buyer_cnt,
order_data.buyer_cnt,
order_data.order_cnt,
order_data.try_order_cnt,
order_data.try_buyer_cnt
FROM
(
select
nvl(region_code, 'all') AS region_code,
nvl(platform, 'all') AS platform,
nvl(payment_name, 'all') AS payment_name,
count(DISTINCT dau) as dau,
count(DISTINCT checkout_new_uv) as checkout_new_uv,
count(DISTINCT checkout_place_order_uv) as checkout_place_order_uv,
count(DISTINCT payment_uv) as payment_uv,
count(DISTINCT checkout_deliver_to_uv) as checkout_deliver_to_uv,
count(DISTINCT checkout_deliver_to_popular_uv) as checkout_deliver_to_popular_uv,
count(DISTINCT checkout_add_address_uv) as checkout_add_address_uv,
count(DISTINCT checkout_address_edit_uv) as checkout_address_edit_uv,
count(DISTINCT checkout_deliver_address_uv) as checkout_deliver_address_uv,
count(DISTINCT checkout_my_bag_uv) as checkout_my_bag_uv,
count(DISTINCT checkout_coupon_uv) as checkout_coupon_uv,
count(DISTINCT checkout_balance_uv) as checkout_balance_uv,
count(DISTINCT checkout_cod_uv) as checkout_cod_uv,
count(DISTINCT checkout_credit_card_uv) as checkout_credit_card_uv,
count(DISTINCT payment_new_card_to_pay_uv) as payment_new_card_to_pay_uv,
count(payment_new_card_to_pay_uv) as payment_new_card_to_pay_pv,
count(DISTINCT payment_confirm_uv) as payment_confirm_uv,
count(DISTINCT payment_card_uv) as payment_card_uv,
count(DISTINCT payment_paypal_uv) as payment_paypal_uv,
count(DISTINCT card_choosing_uv) as card_choosing_uv,
count(DISTINCT payment_checkout_uv) as payment_checkout_uv,
count(payment_checkout_uv) as payment_checkout_pv,
count(DISTINCT deliver_to_back_uv) as deliver_to_back_uv,
count(DISTINCT checkout_new_stay_uv) as checkout_new_stay_uv,
count(DISTINCT add_new_card_stay_uv) as add_new_card_stay_uv,
count(DISTINCT add_new_card_leave_uv) as add_new_card_leave_uv,
count(DISTINCT add_new_card_stay_leave_uv) as add_new_card_stay_leave_uv,
count(DISTINCT checkout_address_edit_stay_uv) as checkout_address_edit_stay_uv,
count(DISTINCT checkout_address_edit_leave_uv) as checkout_address_edit_leave_uv,
count(DISTINCT checkout_address_edit_stay_leave_uv) as checkout_address_edit_stay_leave_uv,
count(DISTINCT payment_stay_uv) as payment_stay_uv,
count(DISTINCT payment_leave_uv) as payment_leave_uv,
count(DISTINCT payment_stay_leave_uv) as payment_stay_leave_uv,
count(DISTINCT checkout_out_of_stock_uv) as checkout_out_of_stock_uv,
count(DISTINCT sku_change_stay_uv) as sku_change_stay_uv,
count(DISTINCT deliver_to_checkout_out_of_stock_uv) as deliver_to_checkout_out_of_stock_uv,
count(DISTINCT deliver_to_sku_change_stay_uv) as deliver_to_sku_change_stay_uv,
count(DISTINCT checkout_sku_change_uv) as checkout_sku_change_uv,
count(DISTINCT checkout_zero_subtotal_uv) as checkout_zero_subtotal_uv,
count(DISTINCT checkout_price_change_uv) as checkout_price_change_uv,
count(DISTINCT intermediate_uv) as intermediate_uv,
count(DISTINCT intermediate_continue_uv) as intermediate_continue_uv,
count(DISTINCT intermediate_edit_uv) as intermediate_edit_uv,
count(DISTINCT intermediate_back_uv) as intermediate_back_uv,
count(DISTINCT intermediate_invalid_uv) as intermediate_invalid_uv,
count(DISTINCT intermediate_exception_uv) as intermediate_exception_uv,
count(DISTINCT payment_success) as payment_success,
count(DISTINCT payment_fail) as payment_fail
from
(
select
nvl(t1.geo_country,'NALL') region_code,
nvl(t1.os_type,'NA') platform,
nvl(order_data.payment_name,'NA') payment_name,
CASE when t1.event_name = 'screen_view' THEN t1.device_id end dau,
CASE when t1.page_code IN ('checkout_new', 'checkout') THEN t1.device_id end checkout_new_uv,
CASE when t1.page_code IN ('checkout_new', 'checkout') AND t1.element_name IN ('checkout_place_order', 'order_placed') THEN t1.device_id end checkout_place_order_uv,
CASE when t1.page_code = 'payment' THEN t1.device_id end payment_uv,
CASE when t1.page_code = 'checkout_new' AND t1.element_name = 'checkout_deliver_to' THEN t1.device_id end checkout_deliver_to_uv,
CASE when t1.page_code IN ('deliver_to') AND t1.element_name IN ('checkout_deliver_to_popular') THEN t1.device_id end checkout_deliver_to_popular_uv,
CASE when t1.page_code IN ('checkout_new') AND t1.element_name IN ('checkout_add_address') THEN t1.device_id end checkout_add_address_uv,
CASE when t1.page_code IN ('checkout_address_edit') AND t1.element_name IN ('edit_country') THEN t1.device_id end checkout_address_edit_uv,
CASE when t1.page_code IN ('checkout_new') AND t1.element_name IN ('checkout_deliver_address') THEN t1.device_id end checkout_deliver_address_uv,
CASE when t1.page_code IN ('checkout_new') AND t1.element_name IN ('checkout_my_bag') THEN t1.device_id end checkout_my_bag_uv,
CASE when t1.page_code IN ('checkout_new') AND t1.element_name IN ('checkout_coupon') THEN t1.device_id end checkout_coupon_uv,
CASE when t1.page_code IN ('checkout_new') AND t1.element_name IN ('checkout_balance') THEN t1.device_id end checkout_balance_uv,
CASE when t1.page_code IN ('checkout_new') AND t1.element_name IN ('checkout_cod') THEN t1.device_id end checkout_cod_uv,
CASE when t1.page_code IN ('add_new_card', 'checkout_credit_card') THEN t1.device_id end checkout_credit_card_uv,
CASE when t1.page_code IN ('add_new_card', 'checkout_credit_card') AND t1.element_name IN ('payment_new_card_to_pay', 'creditCardAddCardClick') THEN t1.device_id end payment_new_card_to_pay_uv,
CASE when t1.page_code IN ('payment') AND t1.element_name IN ('payment_confirm') THEN t1.device_id end payment_confirm_uv,
CASE when t1.page_code IN ('payment', 'checkout_credit_card') AND t1.element_name IN ('payment_card', 'creditCardCardSelect') THEN t1.device_id end payment_card_uv,
CASE when t1.page_code IN ('payment', 'checkout') AND t1.element_name IN ('payment_paypal', 'papalPlaceOrder') THEN t1.device_id end payment_paypal_uv,
CASE when t1.page_code IN ('payment') AND t1.element_name IN ('card_choosing') THEN t1.device_id end card_choosing_uv,
CASE when t1.page_code IN ('payment', 'checkout') THEN t1.device_id end payment_checkout_uv,
CASE when t1.page_code IN ('checkout_new') AND t1.element_name IN ('deliver_to_back') THEN t1.device_id end deliver_to_back_uv,
CASE when t1.page_code IN ('checkout_new') AND t1.element_name IN ('stay') THEN t1.device_id end checkout_new_stay_uv,
CASE when t1.page_code IN ('add_new_card') AND t1.element_name IN ('stay') THEN t1.device_id end add_new_card_stay_uv,
CASE when t1.page_code IN ('add_new_card') AND t1.element_name IN ('leave') THEN t1.device_id end add_new_card_leave_uv,
CASE when t1.page_code IN ('add_new_card') AND t1.element_name IN ('stay', 'leave') THEN t1.device_id end add_new_card_stay_leave_uv,
CASE when t1.page_code IN ('checkout_address_edit') AND t1.element_name IN ('stay') THEN t1.device_id end checkout_address_edit_stay_uv,
CASE when t1.page_code IN ('checkout_address_edit') AND t1.element_name IN ('leave') THEN t1.device_id end checkout_address_edit_leave_uv,
CASE when t1.page_code IN ('checkout_address_edit') AND t1.element_name IN ('stay', 'leave') THEN t1.device_id end checkout_address_edit_stay_leave_uv,
CASE when t1.page_code IN ('payment') AND t1.element_name IN ('stay') THEN t1.device_id end payment_stay_uv,
CASE when t1.page_code IN ('payment') AND t1.element_name IN ('leave') THEN t1.device_id end payment_leave_uv,
CASE when t1.page_code IN ('payment') AND t1.element_name IN ('stay', 'leave') THEN t1.device_id end payment_stay_leave_uv,
CASE when t1.page_code IN ('checkout_new') AND t1.element_name IN ('checkout_out_of_stock') THEN t1.device_id end checkout_out_of_stock_uv,
CASE when t1.page_code IN ('checkout_new') AND t1.element_name IN ('sku_change_stay') THEN t1.device_id end sku_change_stay_uv,
CASE when t1.page_code IN ('deliver_to') AND t1.element_name IN ('checkout_out_of_stock') THEN t1.device_id end deliver_to_checkout_out_of_stock_uv,
CASE when t1.page_code IN ('deliver_to') AND t1.element_name IN ('sku_change_stay') THEN t1.device_id end deliver_to_sku_change_stay_uv,
CASE when t1.page_code IN ('checkout_new') AND t1.element_name IN ('checkout_sku_change') THEN t1.device_id end checkout_sku_change_uv,
CASE when t1.page_code IN ('checkout_new') AND t1.element_name IN ('checkout_zero_subtotal') THEN t1.device_id end checkout_zero_subtotal_uv,
CASE when t1.page_code IN ('checkout_new') AND t1.element_name IN ('checkout_price_change') THEN t1.device_id end checkout_price_change_uv,
CASE when t1.page_code IN ('intermediate') THEN t1.device_id end intermediate_uv,
CASE when t1.page_code IN ('intermediate') AND t1.element_name IN ('intermediate_continue') THEN t1.device_id end intermediate_continue_uv,
CASE when t1.page_code IN ('intermediate') AND t1.element_name IN ('intermediate_edit') THEN t1.device_id end intermediate_edit_uv,
CASE when t1.page_code IN ('intermediate') AND t1.element_name IN ('intermediate_back') THEN t1.device_id end intermediate_back_uv,
CASE when t1.page_code IN ('intermediate') AND t1.element_name IN ('intermediate_invalid') THEN t1.device_id end intermediate_invalid_uv,
CASE when t1.page_code IN ('intermediate') AND t1.element_name IN ('intermediate_exception') THEN t1.device_id end intermediate_exception_uv,
CASE when t1.page_code IN ('payment_success') THEN t1.device_id end payment_success,
CASE when t1.page_code IN ('payment_fail') THEN t1.device_id end payment_fail
from
(
select pt,datasource,event_name,geo_country,os_type,page_code,device_id,NULL element_name from dwd.dwd_vova_log_screen_view where pt='${cur_date}' and platform ='mob' and datasource= 'vova'
union all
select pt,datasource,event_name,geo_country,os_type,page_code,device_id,element_name from dwd.dwd_vova_log_common_click where pt='${cur_date}' and platform ='mob' and datasource= 'vova'
union all
select pt,datasource,event_name,geo_country,os_type,page_code,device_id,NULL element_name from dwd.dwd_vova_log_page_view where pt='${cur_date}' and platform ='mob' and datasource= 'vova'
) t1
LEFT JOIN (
SELECT device_id,
first_value(payment_name) AS payment_name
FROM
(
SELECT
ddog.device_id,
CASE WHEN ocir.order_sn IS NOT NULL
THEN 'dlocal_installment'
ELSE ddog.payment_name
END AS payment_name
FROM
dim.dim_vova_order_goods ddog
LEFT JOIN ods_vova_vts.ods_vova_order_card_installments_record ocir ON ocir.order_sn = ddog.order_sn
WHERE DATE(ddog.order_time) = '${cur_date}'
  AND ddog.parent_order_id = 0
) temp
GROUP BY device_id
) order_data ON order_data.device_id = t1.device_id
)log_data
GROUP BY CUBE (log_data.region_code, log_data.platform, log_data.payment_name)
) log_data
LEFT JOIN
(
select
nvl(nvl(payment_name,'NA'), 'all') AS payment_name,
nvl(nvl(platform,'NA'), 'all') AS platform,
nvl(nvl(region_code,'NALL'), 'all') AS region_code,
SUM(gmv) AS gmv,
count(DISTINCT paid_order_cnt) AS paid_order_cnt,
count(DISTINCT paid_buyer_cnt) AS paid_buyer_cnt,
count(DISTINCT buyer_id) AS buyer_cnt,
count(DISTINCT order_id) AS order_cnt,
count(DISTINCT try_order_cnt) AS try_order_cnt,
count(DISTINCT try_buyer_cnt) AS try_buyer_cnt
FROM
(
SELECT
CASE WHEN ocir.order_sn IS NOT NULL
THEN 'dlocal_installment'
ELSE ddog.payment_name
END AS payment_name,
ddog.platform,
ddog.region_code,
if(ddog.pay_status >= 1, ddog.shop_price * ddog.goods_number + ddog.shipping_fee, 0) AS gmv,
if(ddog.pay_status >= 1, ddog.order_id, null) AS paid_order_cnt,
if(ddog.pay_status >= 1, ddog.buyer_id, null) AS paid_buyer_cnt,
ddog.order_id,
ddog.buyer_id,
if(pt.order_sn is not null, ddog.order_id, null) AS try_order_cnt,
if(pt.order_sn is not null, ddog.buyer_id, null) AS try_buyer_cnt
FROM
dim.dim_vova_order_goods ddog
LEFT JOIN ods_vova_vts.ods_vova_order_card_installments_record ocir ON ocir.order_sn = ddog.order_sn
LEFT JOIN (SELECT order_sn FROM ods_vova_vts.ods_vova_paypal_txn pt group by order_sn) pt ON pt.order_sn = ddog.order_sn
WHERE DATE(ddog.order_time) = '${cur_date}'
  AND ddog.parent_order_id = 0
) temp
GROUP BY CUBE (nvl(payment_name,'NA'), nvl(platform,'NA'), nvl(region_code,'NALL'))
) order_data
ON log_data.payment_name = order_data.payment_name
AND log_data.platform = order_data.platform
AND log_data.region_code = order_data.region_code
"

#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=30" --conf "spark.dynamicAllocation.initialExecutors=60" --conf "spark.app.name=dwb_vova_checkout" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

