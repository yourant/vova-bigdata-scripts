#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
##dependence
#dwd_vova_log_common_click
#dim_vova_order_goods
##tmp_dwb_vova_payment_error_code
#ods_vova_order_card_installments_record
#ods_vova_paypal_txn
#ods_vova_order_info



sql="

INSERT OVERWRITE TABLE dwb.dwb_vova_payment PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
'${cur_date}' AS event_date,
pay_final.datasource,
pay_final.region_code,
pay_final.platform,
pay_final.payment_name,
pay_final.order_count,
pay_final.user_count,
pay_final.pay_success_order_count,
pay_final.pay_success_user_count,
nvl(pay_final.try_order_count, 0) as try_order_count,
nvl(pay_final.try_order_count_pv, 0) as try_order_count_pv,
pay_final.try_user_count,
pay_final.try_insufficient_amount_order_count,
pay_final.try_insufficient_amount_user_count,
last_data.pay_success_order_count AS last_pay_success_order_count,
last_data.try_order_count AS last_try_order_count,
0 as log_try_pv,
0 as log_try_uv,
concat(round(nvl((nvl(pay_final.try_order_count, 0)) / pay_final.order_count, 0) * 100 ,2 ),'%') AS try_order_count_div_order_count,
concat(round(nvl(pay_final.try_user_count / pay_final.user_count, 0) * 100 ,2 ),'%') AS try_user_count_div_user_count,
concat(round(nvl(pay_final.pay_success_order_count / (nvl(pay_final.try_order_count, 0)), 0) * 100 ,2 ),'%') AS pay_success_order_count_div_try_order_count,
concat(round(nvl(pay_final.pay_success_order_count / (nvl(pay_final.try_order_count, 0)) - last_data.pay_success_order_count / last_data.try_order_count, 0) * 100 ,2 ),'%') AS pay_success_order_count_div_try_order_count_compare_last,
concat(round(nvl(pay_final.pay_success_order_count / pay_final.order_count, 0) * 100 ,2 ),'%') AS pay_success_order_count_div_order_count,
if(pay_final.payment_name = 'Cash_On_Delivery',concat(round(nvl(pay_final.pay_success_order_count / (nvl(pay_final.try_order_count, 0)), 0) * 100 ,2 ),'%'),concat(round(nvl(pay_final.pay_success_order_count / pay_final.try_insufficient_amount_order_count, 0) * 100 ,2 ),'%')) AS pay_success_order_count_div_try_insufficient_amount_order_count,
concat(round(nvl(pay_final.pay_success_order_count / pay_final.try_order_count_pv, 0) * 100 ,2 ),'%') AS pay_success_order_count_div_try_order_count_pv
from
(
select
nvl(region_code, 'all') AS region_code,
nvl(payment_name, 'all') AS payment_name,
nvl(datasource, 'all') AS datasource,
nvl(platform, 'all') AS platform,
count(distinct order_sn) AS order_count,
count(distinct buyer_id) AS user_count,
count(distinct pay_success_order) AS pay_success_order_count,
count(distinct pay_success_buyer) AS pay_success_user_count,
count(distinct try_order) AS try_order_count,
sum(try_order_pv) AS try_order_count_pv,
count(distinct try_user) AS try_user_count,
count(distinct try_insufficient_amount_order) AS try_insufficient_amount_order_count,
count(distinct try_insufficient_amount_user) AS try_insufficient_amount_user_count
from
(
select
'${cur_date}' AS event_date,
ddog.region_code,
CASE WHEN ocir.order_sn IS NOT NULL
THEN 'dlocal_installment'
ELSE ddog.payment_name
END AS payment_name,
ddog.order_sn,
ddog.buyer_id,
if(ddog.pay_status >= 1, ddog.order_sn, null) pay_success_order,
if(ddog.pay_status >= 1, ddog.buyer_id, null) pay_success_buyer,
if(ddog.payment_id != 220, if(pt.order_sn is not null, ddog.order_sn, null), if(log_data.device_id is not null, ddog.order_sn, if(ddog.pay_status >= 1, ddog.order_sn, null))) try_order,
if(ddog.payment_id != 220, if(pt.order_sn is not null, pt.try_cnt, 0), if(log_data.device_id is not null, log_data.confirm_pv, 0)) try_order_pv,
if(ddog.payment_id != 220, if(pt.order_sn is not null, ddog.buyer_id, null), if(log_data.device_id is not null, ddog.buyer_id, if(ddog.pay_status >= 1, ddog.buyer_id, null))) try_user,
-- if(pt.order_sn is not null, ddog.order_sn, null) try_order,
-- if(pt.order_sn is not null, pt.try_cnt, 0) try_order_pv,
-- if(pt.order_sn is not null, ddog.buyer_id, null) try_user,
if(ddog.payment_id != 220, if(pt.order_sn is not null AND pay_error_data.order_sn is null, ddog.order_sn, null), null) try_insufficient_amount_order,
if(ddog.payment_id != 220, if(pt.order_sn is not null AND pay_error_data.order_sn is null, ddog.buyer_id, null), null) try_insufficient_amount_user,
ddog.datasource,
ddog.platform
from
(
SELECT
ddog.order_id,
first(order_sn) AS order_sn,
first(region_code) AS region_code,
first(buyer_id) AS buyer_id,
first(pay_status) AS pay_status,
first(payment_id) AS payment_id,
first(payment_name) AS payment_name,
first(device_id) AS device_id,
first(datasource) AS datasource,
first(platform) AS platform
FROM
dim.dim_vova_order_goods ddog
WHERE
date(ddog.order_time) = '${cur_date}'
AND ddog.parent_order_id = 0
GROUP BY ddog.order_id
) ddog
LEFT JOIN ods_vova_vts.ods_vova_order_card_installments_record ocir ON ocir.order_sn = ddog.order_sn
LEFT JOIN (SELECT order_sn, count(*) AS try_cnt FROM ods_vova_vts.ods_vova_paypal_txn group by order_sn) pt ON pt.order_sn = ddog.order_sn
LEFT JOIN (SELECT DISTINCT order_sn FROM tmp.tmp_dwb_vova_payment_error_code WHERE event_date = '${cur_date}' AND error_code IN (20051, 430365, 2001, 1002, 5120, 10009, 10321, 13602, 17200) ) pay_error_data ON pay_error_data.order_sn = ddog.order_sn
LEFT JOIN
(
SELECT
log.device_id,
count(*) AS confirm_pv
FROM dwd.dwd_vova_log_common_click log
WHERE log.pt >= '${cur_date}'
  AND log.pt < date_add('${cur_date}', 7)
  AND log.page_code = 'payment'
  AND log.element_name = 'payment_confirm'
  AND log.datasource = 'vova'
GROUP BY log.device_id
) log_data ON ddog.device_id = log_data.device_id
) pay_data
group by cube (region_code, payment_name, datasource, platform)
) pay_final
LEFT JOIN
(
SELECT
datasource,
region_code,
platform,
payment_name,
pay_success_order_count,
try_order_count
FROM dwb.dwb_vova_payment
WHERE pt = date_sub('${cur_date}', 1)
) last_data ON pay_final.datasource = last_data.datasource
AND pay_final.region_code = last_data.region_code
AND pay_final.platform = last_data.platform
AND pay_final.payment_name = last_data.payment_name

;



"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=dwb_vova_payment" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.autoBroadcastJoinThreshold=-1" \
-e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi