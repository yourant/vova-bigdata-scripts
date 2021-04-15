#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 month" +%Y-%m-01`
fi
echo "$cur_date"
###get clone data
#finance gmv
sql="
DROP TABLE IF EXISTS tmp.tmp_dwb_vova_refund_time;
CREATE TABLE tmp.tmp_dwb_vova_refund_time AS
select
sol.order_goods_id,
max(sol.create_time) AS refund_time
from
ods_vova_vts.ods_vova_sku_ops_log sol
where sol.ops='sku_pay_status' AND sol.status=4
group by sol.order_goods_id
;

INSERT OVERWRITE TABLE dwb.dwb_vova_finance_gmv PARTITION (pt = '${cur_date}')
select
concat(trunc('${cur_date}','MM'),'---', last_day('${cur_date}')) AS event_date,
if(final_data.mct_id = 'all', 'all', REPLACE(dm.mct_name, ',', ' ')) AS mct_name,
final_data.source,
final_data.payment_name,
final_data.total_shop_price_amount,
final_data.total_shipping_fee,
final_data.total_bonus,
final_data.total_receive_amount,
final_data.merchant_duty_refund_amount,
final_data.non_merchant_duty_refund_amount,
final_data.total_refund_amount,
final_data.total_refund_bonus,
final_data.total_actual_refund_amount,
final_data.brand_value_added,
final_data.refund_brand_value_added,
final_data.total_container_transportation_shipping_fee,
final_data.total_refund_container_transportation_shipping_fee,
final_data.auction_value_added,
final_data.refund_auction_value_added,
final_data.lucky_order_amount,
final_data.lucky_bonus
FROM
(
select
receive_data.event_date,
receive_data.mct_id,
source,
payment_name,
sum(receive_data.total_shop_price_amount) AS total_shop_price_amount,
sum(receive_data.total_shipping_fee) AS total_shipping_fee,
sum(receive_data.total_bonus) AS total_bonus,
sum(receive_data.total_receive_amount) AS total_receive_amount,
sum(receive_data.merchant_duty_refund_amount) AS merchant_duty_refund_amount,
sum(receive_data.non_merchant_duty_refund_amount) AS non_merchant_duty_refund_amount,
sum(receive_data.total_refund_amount) AS total_refund_amount,
sum(receive_data.total_refund_bonus) AS total_refund_bonus,
sum(receive_data.total_actual_refund_amount) AS total_actual_refund_amount,
sum(receive_data.brand_value_added) AS brand_value_added,
sum(receive_data.refund_brand_value_added) AS refund_brand_value_added,
sum(receive_data.total_container_transportation_shipping_fee) AS total_container_transportation_shipping_fee,
sum(receive_data.total_refund_container_transportation_shipping_fee) AS total_refund_container_transportation_shipping_fee,
sum(receive_data.auction_value_added) AS auction_value_added,
sum(receive_data.refund_auction_value_added) AS refund_auction_value_added,
sum(receive_data.lucky_order_amount) AS lucky_order_amount,
sum(receive_data.lucky_bonus) AS lucky_bonus
from
(
select
nvl(date(dog.receive_time), 'all') AS event_date,
nvl(concat(dog.datasource , ' ', dog.order_source), 'all') AS source,
nvl(payment_name, 'all') AS payment_name,
nvl(mct_id, 'all') AS mct_id,
sum(dog.shop_price * dog.goods_number) AS total_shop_price_amount,
sum(dog.shipping_fee) AS total_shipping_fee,
sum(dog.bonus) AS total_bonus,
sum(dog.shop_price * dog.goods_number + dog.shipping_fee + dog.bonus) AS total_receive_amount,
sum(dog.container_transportation_shipping_fee) AS total_container_transportation_shipping_fee,
sum(if(dog.order_tag = '[auction_activity_id]',dog.shop_price * dog.goods_number + dog.shipping_fee - dog.mct_shop_price * dog.goods_number - dog.mct_shipping_fee, 0)) AS auction_value_added,
sum(if(dog.order_tag != '[auction_activity_id]' OR dog.order_tag is NULL,dog.shop_price * dog.goods_number + dog.shipping_fee - dog.mct_shop_price * dog.goods_number - dog.mct_shipping_fee, 0)) AS brand_value_added,
 0 AS total_refund_amount,
 0 AS total_refund_bonus,
 0 AS total_actual_refund_amount,
 0 AS total_refund_container_transportation_shipping_fee,
 0 AS merchant_duty_refund_amount,
 0 AS non_merchant_duty_refund_amount,
 0 AS refund_auction_value_added,
 0 AS refund_brand_value_added,
 0 AS lucky_order_amount,
 0 AS lucky_bonus
from
(
select
dog.receive_time,
dog.datasource,
dog.order_source,
dog.payment_name,
dog.shop_price,
dog.goods_number,
dog.shipping_fee,
dog.bonus,
dog.order_tag,
dog.mct_shop_price,
dog.mct_shipping_fee,
dog.container_transportation_shipping_fee,
if(dog2.order_goods_id is null, dog.mct_id, dog2.mct_id) AS mct_id
from dim.dim_vova_order_goods dog
LEFT JOIN dim.dim_vova_order_goods dog2 on dog2.parent_rec_id = dog.order_goods_id AND dog2.parent_rec_id > 0
WHERE dog.pay_status = 2
AND date(dog.receive_time) >= trunc('${cur_date}','MM')
AND date(dog.receive_time) <= last_day('${cur_date}')
AND dog.parent_rec_id = 0
AND dog.payment_id NOT IN (232, 233)

UNION ALL

select
dog.receive_time,
dog.datasource,
dog.order_source,
dog.payment_name,
dog.shop_price,
dog.goods_number,
dog.shipping_fee,
dog.bonus,
dog.order_tag,
dog.mct_shop_price,
dog.mct_shipping_fee,
dog.container_transportation_shipping_fee,
if(dog2.order_goods_id is null, dog.mct_id, dog2.mct_id) AS mct_id
from dim.dim_vova_order_goods dog
LEFT JOIN dim.dim_vova_order_goods dog2 on dog2.parent_rec_id = dog.order_goods_id AND dog2.parent_rec_id > 0
INNER JOIN ods_vova_vts.ods_vova_adyen_klarna_capture_record kcr on kcr.order_sn = dog.order_sn
WHERE dog.pay_status != 11
AND dog.pay_status > 1
AND date(kcr.capture_time) >= trunc('${cur_date}','MM')
AND date(kcr.capture_time) <= last_day('${cur_date}')
AND dog.parent_rec_id = 0
AND dog.payment_id IN (232, 233)

) dog
GROUP BY CUBE (date(dog.receive_time),concat(dog.datasource , ' ', dog.order_source),dog.payment_name,dog.mct_id)
UNION ALL
select
nvl(date(tt.refund_time), 'all') AS event_date,
nvl(concat(dog.datasource , ' ', dog.order_source), 'all') AS source,
nvl(dog.payment_name, 'all') AS payment_name,
nvl(dog.mct_id, 'all') AS mct_id,
 0 AS total_shop_price_amount,
 0 AS total_shipping_fee,
 0 AS total_bonus,
 0 AS total_receive_amount,
 0 AS total_container_transportation_shipping_fee,
 0 AS auction_value_added,
 0 AS brand_value_added,
sum(rr.refund_amount) AS total_refund_amount,
sum(rr.bonus) AS total_refund_bonus,
sum(rr.refund_amount + rr.bonus) AS total_actual_refund_amount,
sum(dog.container_transportation_shipping_fee) AS total_refund_container_transportation_shipping_fee,
sum(if(rr.refund_type_id in(5,6,11,14) OR (dog.sku_shipping_status > 0 AND rr.refund_type_id = 12) OR (rr.refund_type_id = 2 AND refund_reason_type_id !=8 ), rr.refund_amount, 0)) AS merchant_duty_refund_amount,
sum(rr.refund_amount) - sum(if(rr.refund_type_id in(5,6,11,14) OR (dog.sku_shipping_status > 0 AND rr.refund_type_id = 12) OR (rr.refund_type_id = 2 AND refund_reason_type_id !=8 ), rr.refund_amount, 0)) AS non_merchant_duty_refund_amount,
sum(if(dog.order_tag = '[auction_activity_id]',dog.shop_price * dog.goods_number + dog.shipping_fee - dog.mct_shop_price * dog.goods_number - dog.mct_shipping_fee, 0)) AS refund_auction_value_added,
sum(if(dog.order_tag != '[auction_activity_id]' OR dog.order_tag is NULL,dog.shop_price * dog.goods_number + dog.shipping_fee - dog.mct_shop_price * dog.goods_number - dog.mct_shipping_fee, 0)) AS refund_brand_value_added,
0 AS lucky_order_amount,
0 AS lucky_bonus
from
dwd.dwd_vova_fact_refund rr
INNER JOIN dim.dim_vova_order_goods dog on rr.order_goods_id = dog.order_goods_id
INNER JOIN tmp.tmp_dwb_vova_refund_time tt on tt.order_goods_id = dog.order_goods_id
where dog.sku_pay_status=4
AND date(tt.refund_time) >= trunc('${cur_date}','MM')
AND date(tt.refund_time) <= last_day('${cur_date}')
GROUP BY CUBE (date(tt.refund_time),concat(dog.datasource , ' ', dog.order_source),dog.payment_name,dog.mct_id)
UNION ALL
select
nvl(date(fag.rcv_time), 'all')      AS event_date,
'vova app'  AS source,
nvl(dp.payment_name, 'all') AS payment_name,
nvl(dg.mct_id, 'all') AS mct_id,
 0 AS total_shop_price_amount,
 0 AS total_shipping_fee,
 0 AS total_bonus,
 0 AS total_receive_amount,
 0 AS total_container_transportation_shipping_fee,
 0 AS auction_value_added,
 0 AS brand_value_added,
 0 AS total_refund_amount,
 0 AS total_refund_bonus,
 0 AS total_actual_refund_amount,
 0 AS total_refund_container_transportation_shipping_fee,
 0 AS merchant_duty_refund_amount,
 0 AS non_merchant_duty_refund_amount,
 0 AS refund_auction_value_added,
 0 AS refund_brand_value_added,
 sum(fag.ord_amt + fag.ship_fee) AS lucky_order_amount,
 sum(fag.bonus)                  AS lucky_bonus
FROM dwd.dwd_vova_fact_act_ord_gs fag
INNER JOIN dim.dim_vova_payment dp ON dp.payment_id = fag.pmt_id
INNER JOIN dim.dim_vova_goods dg ON dg.goods_id = fag.gs_id
WHERE fag.pay_sts = 2
  AND fag.ord_id = 0
  AND fag.rcv_time >= trunc('${cur_date}','MM')
  AND fag.rcv_time <= last_day('${cur_date}')
GROUP BY CUBE (date(fag.rcv_time),dp.payment_name,dg.mct_id)
) receive_data
GROUP BY event_date,source,payment_name,mct_id
) final_data
LEFT JOIN dim.dim_vova_merchant dm on dm.mct_id = final_data.mct_id
WHERE final_data.event_date = 'all'
AND final_data.payment_name != 'all'
AND final_data.source != 'all'

;
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=finance_gmv" -e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi