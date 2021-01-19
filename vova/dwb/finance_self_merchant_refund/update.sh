#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

job_name="dwb_vova_finance_self_merchant_refund_req4435_chenkai_${cur_date}"

sql="
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE dwb.dwb_vova_finance_self_merchant_refund PARTITION (pt)
select
/*+ REPARTITION(1) */
event_date,
mct_name,
confirm_order_cnt,
confirm_mct_amount,
cancel_order_cnt,
cancel_mct_amount,
cancel_purchase_order_cnt,
cancel_purchase_mct_amount,
cancel_purchase_total_amount,
shipping_order_cnt,
shipping_mct_amount,
not_shipping_order_cnt,
not_shipping_mct_amount,
cancel_shipping_order_cnt,
cancel_shipping_mct_amount,
nvl(cancel_order_cnt / confirm_order_cnt, 0) AS cancel_order_cnt_div_confirm_order_cnt,
nvl(cancel_purchase_order_cnt / confirm_order_cnt, 0) AS cancel_purchase_order_cnt_div_confirm_order_cnt,
nvl(shipping_order_cnt / (confirm_order_cnt - cancel_order_cnt), 0) AS shipping_order_cnt_div_confirm_order_cnt_b,
nvl(cancel_shipping_order_cnt / shipping_order_cnt, 0) AS cancel_shipping_order_cnt_div_shipping_order_cnt,
event_date AS pt
from
(
select
date(dog.confirm_time) as event_date,
dm.mct_name,
count(dog.order_goods_id) AS confirm_order_cnt,
sum(dog.mct_shop_price * dog.goods_number + dog.mct_shipping_fee) AS confirm_mct_amount,
count(if(dog.sku_pay_status = 4 and dog.sku_shipping_status = 0, dog.order_goods_id, null)) AS cancel_order_cnt,
sum(if(dog.sku_pay_status = 4 and dog.sku_shipping_status = 0, dog.mct_shop_price * dog.goods_number + dog.mct_shipping_fee, 0)) AS cancel_mct_amount,
count(if(dog.sku_pay_status = 4 and dog.sku_shipping_status = 0 and rf.purchase_pay_status = 2 , dog.order_goods_id, null)) AS cancel_purchase_order_cnt,
sum(if(dog.sku_pay_status = 4 and dog.sku_shipping_status = 0 and rf.purchase_pay_status = 2 , dog.mct_shop_price * dog.goods_number + dog.mct_shipping_fee, 0)) AS cancel_purchase_mct_amount,
sum(if(dog.sku_pay_status = 4 and dog.sku_shipping_status = 0 and rf.purchase_pay_status = 2 , rf.purchase_total_amount, 0)) AS cancel_purchase_total_amount,
count(if(dog.sku_shipping_status >= 1 , dog.order_goods_id, null)) AS shipping_order_cnt,
sum(if(dog.sku_shipping_status >= 1 , dog.mct_shop_price * dog.goods_number + dog.mct_shipping_fee, 0)) AS shipping_mct_amount,
count(if(dog.sku_shipping_status = 0 , dog.order_goods_id, null)) AS not_shipping_order_cnt,
sum(if(dog.sku_shipping_status = 0 , dog.mct_shop_price * dog.goods_number + dog.mct_shipping_fee, 0)) AS not_shipping_mct_amount,
count(if(dog.sku_shipping_status >= 1 and dog.sku_pay_status = 4 , dog.order_goods_id, null)) AS cancel_shipping_order_cnt,
sum(if(dog.sku_shipping_status >= 1 and dog.sku_pay_status = 4 , dog.mct_shop_price * dog.goods_number + dog.mct_shipping_fee, 0)) AS cancel_shipping_mct_amount
from
dim.dim_vova_order_goods dog
LEFT JOIN dim.dim_vova_merchant dm on dm.mct_id = dog.mct_id
LEFT JOIN dwb.dwb_vova_financial_self_process rf on rf.order_goods_sn = dog.order_goods_sn
WHERE dog.pay_status >= 1
AND dog.sku_order_status != 5
AND date(dog.confirm_time) >= date_sub('${cur_date}', 90)
AND date(dog.confirm_time) <= '${cur_date}'
AND dog.mct_id IN (26414)
GROUP BY date(dog.confirm_time),dm.mct_name
) final_data

UNION ALL

select
event_date,
mct_name,
confirm_order_cnt,
confirm_mct_amount,
cancel_order_cnt,
cancel_mct_amount,
cancel_purchase_order_cnt,
cancel_purchase_mct_amount,
cancel_purchase_total_amount,
shipping_order_cnt,
shipping_mct_amount,
not_shipping_order_cnt,
not_shipping_mct_amount,
cancel_shipping_order_cnt,
cancel_shipping_mct_amount,
nvl(cancel_order_cnt / confirm_order_cnt, 0) AS cancel_order_cnt_div_confirm_order_cnt,
nvl(cancel_purchase_order_cnt / confirm_order_cnt, 0) AS cancel_purchase_order_cnt_div_confirm_order_cnt,
nvl(shipping_order_cnt / (confirm_order_cnt - cancel_order_cnt), 0) AS shipping_order_cnt_div_confirm_order_cnt_b,
nvl(cancel_shipping_order_cnt / shipping_order_cnt, 0) AS cancel_shipping_order_cnt_div_shipping_order_cnt,
event_date AS pt
from
(
select
date(dog.confirm_time) as event_date,
dm.mct_name,
count(dog.order_goods_id) AS confirm_order_cnt,
sum(dog.mct_shop_price * dog.goods_number + dog.mct_shipping_fee) AS confirm_mct_amount,
count(if(dog.sku_pay_status = 4 and (fspog.order_goods_shipping_status < 8 or fspog.order_goods_shipping_status is null), dog.order_goods_id, null)) AS cancel_order_cnt,
sum(if(dog.sku_pay_status = 4 and (fspog.order_goods_shipping_status < 8 or fspog.order_goods_shipping_status is null), dog.mct_shop_price * dog.goods_number + dog.mct_shipping_fee, 0)) AS cancel_mct_amount,
count(if(dog.sku_pay_status = 4 and (fspog.order_goods_shipping_status < 8 or fspog.order_goods_shipping_status is null) and fspog.purchase_order_pay_status = 2 , dog.order_goods_id, null)) AS cancel_purchase_order_cnt,
sum(if(dog.sku_pay_status = 4 and (fspog.order_goods_shipping_status < 8 or fspog.order_goods_shipping_status is null) and fspog.purchase_order_pay_status = 2 , dog.mct_shop_price * dog.goods_number + dog.mct_shipping_fee, 0)) AS cancel_purchase_mct_amount,
sum(if(dog.sku_pay_status = 4 and (fspog.order_goods_shipping_status < 8 or fspog.order_goods_shipping_status is null) and fspog.purchase_order_pay_status = 2 , fspog.purchase_amount, 0)) AS cancel_purchase_total_amount,
count(if(fspog.order_goods_shipping_status >= 8 , dog.order_goods_id, null)) AS shipping_order_cnt,
sum(if(fspog.order_goods_shipping_status >= 8 , dog.mct_shop_price * dog.goods_number + dog.mct_shipping_fee, 0)) AS shipping_mct_amount,
count(if(fspog.order_goods_shipping_status < 8 or fspog.order_goods_shipping_status is null , dog.order_goods_id, null)) AS not_shipping_order_cnt,
sum(if(fspog.order_goods_shipping_status < 8 or fspog.order_goods_shipping_status is null , dog.mct_shop_price * dog.goods_number + dog.mct_shipping_fee, 0)) AS not_shipping_mct_amount,
count(if(fspog.order_goods_shipping_status >= 8 and dog.sku_pay_status = 4 , dog.order_goods_id, null)) AS cancel_shipping_order_cnt,
sum(if(fspog.order_goods_shipping_status >= 8 and dog.sku_pay_status = 4 , dog.mct_shop_price * dog.goods_number + dog.mct_shipping_fee, 0)) AS cancel_shipping_mct_amount
from
dim.dim_vova_order_goods dog
LEFT JOIN dim.dim_vova_merchant dm on dm.mct_id = dog.mct_id
LEFT JOIN dwd.dwd_vova_fact_supply_order_goods fspog on dog.order_goods_sn = fspog.channel_order_goods_sn
WHERE dog.pay_status >= 1
AND dog.sku_order_status != 5
AND date(dog.confirm_time) >= date_sub('${cur_date}', 90)
AND date(dog.confirm_time) <= '${cur_date}'
AND dog.mct_id IN (11630, 36655)
GROUP BY date(dog.confirm_time),dm.mct_name
) final_data
;

INSERT OVERWRITE TABLE dwb.dwb_vova_finance_self_merchant_refund_detail PARTITION (pt)
select
date(dog.confirm_time) as event_date,
dm.mct_name,
rt.refund_type,
count(dog.order_goods_id) AS refund_order_cnt,
sum(dog.mct_shop_price * dog.goods_number + dog.mct_shipping_fee) AS refund_mct_amount,
sum(fr.refund_amount) AS refund_amount,
date(dog.confirm_time) as pt
from
dim.dim_vova_order_goods dog
INNER JOIN dwd.dwd_vova_fact_refund fr on dog.order_goods_id = fr.order_goods_id
LEFT JOIN ods_vova_vts.ods_vova_refund_type rt on rt.refund_type_id = fr.refund_type_id
LEFT JOIN dim.dim_vova_merchant dm on dm.mct_id = dog.mct_id
WHERE dog.pay_status >= 1
AND dog.sku_order_status != 5
AND dog.sku_pay_status = 4
AND dog.sku_shipping_status = 0
AND date(dog.confirm_time) >= date_sub('${cur_date}', 90)
AND date(dog.confirm_time) <= '${cur_date}'
AND dog.mct_id IN (26414)
GROUP BY date(dog.confirm_time),dm.mct_name,rt.refund_type

UNION ALL

select
date(dog.confirm_time) as event_date,
dm.mct_name,
rt.refund_type,
count(dog.order_goods_id) AS refund_order_cnt,
sum(dog.mct_shop_price * dog.goods_number + dog.mct_shipping_fee) AS refund_mct_amount,
sum(fr.refund_amount) AS refund_amount,
date(dog.confirm_time) as pt
from
dim.dim_vova_order_goods dog
INNER JOIN dwd.dwd_vova_fact_refund fr on dog.order_goods_id = fr.order_goods_id
LEFT JOIN dwd.dwd_vova_fact_supply_order_goods fspog on dog.order_goods_sn = fspog.channel_order_goods_sn
LEFT JOIN ods_vova_vts.ods_vova_refund_type rt on rt.refund_type_id = fr.refund_type_id
LEFT JOIN dim.dim_vova_merchant dm on dm.mct_id = dog.mct_id
WHERE dog.pay_status >= 1
AND dog.sku_order_status != 5
AND dog.sku_pay_status = 4
AND date(dog.confirm_time) >= date_sub('${cur_date}', 90)
AND date(dog.confirm_time) <= '${cur_date}'
AND (fspog.order_goods_shipping_status < 8 or fspog.order_goods_shipping_status is null)
AND dog.mct_id IN (11630, 36655)
GROUP BY date(dog.confirm_time),dm.mct_name,rt.refund_type

;
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=${job_name}" -e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi