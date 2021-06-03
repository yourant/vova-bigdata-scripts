#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
#dependence
#dwd_vova_fact_logistics
#dim_vova_order_goods
#dim_vova_devices
#ods_vova_vts.ods_vova_order_info
#ods_vova_vts.ods_vova_order_extension
#ods_vova_vts.ods_vova_order_goods_extension
#ods_vova_vts.ods_vova_shipping_method
sql="
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE dwb.dwb_vova_tw_order PARTITION (pt)
SELECT
/*+ REPARTITION(1) */
pt AS event_date,
platform,
mct_type,
channel,
order_cnt,
paid_order_cnt,
paid_order_goods_cnt,
paid_buyer_cnt,
gmv,
receive_order_goods_cnt,
total_delivered_days,
paid_cod_order_goods_cnt,
receive_order_goods_cod_cnt,
discount_order_goods_cnt,
container_transportation_shipping_fee_discount,
total_shipping_fee,
nvl(paid_order_cnt / order_cnt, 0) AS paid_order_cnt_div_order_cnt,
nvl(paid_order_goods_cnt / paid_buyer_cnt, 0) AS paid_order_goods_cnt_div_paid_buyer_cnt,
nvl(gmv / paid_buyer_cnt, 0) AS gmv_div_paid_buyer_cnt,
nvl(gmv / paid_order_cnt, 0) AS gmv_div_paid_order_goods_cnt,
nvl(receive_order_goods_cnt / paid_order_goods_cnt, 0) AS receive_order_goods_cnt_div_paid_order_goods_cnt,
nvl(total_delivered_days / total_delivered_cnt, 0) AS avg_receive_days,
nvl(paid_cod_order_goods_cnt / paid_order_goods_cnt, 0) AS paid_cod_order_goods_cnt_div_paid_order_goods_cnt,
nvl(receive_order_goods_cod_cnt / paid_cod_order_goods_cnt, 0) AS receive_order_goods_cod_cnt_div_paid_cod_order_goods_cnt,
nvl(discount_order_goods_cnt / paid_order_goods_cnt, 0) AS discount_order_goods_cnt_div_paid_order_goods_cnt,
total_delivered_cnt,
pt
from
(
SELECT
nvl(date(dog.order_time), 'all') AS pt,
nvl(dog.platform, 'all') AS platform,
nvl(dog.mct_type, 'all') AS mct_type,
nvl(nvl(dd.child_channel, 'NA'), 'all') AS channel,
count(distinct dog.order_id) AS order_cnt,
count(distinct if(dog.pay_status >= 1, dog.order_id, null)) AS paid_order_cnt,
count(distinct if(dog.pay_status >= 1, dog.order_goods_id, null)) AS paid_order_goods_cnt,
count(distinct if(dog.pay_status >= 1, dog.buyer_id, null)) AS paid_buyer_cnt,
sum(if(dog.pay_status >= 1, dog.shop_price * dog.goods_number + dog.shipping_fee, 0)) AS gmv,
sum(if(dog.pay_status >= 1 and dog.sku_shipping_status = 2, 1, 0)) AS receive_order_goods_cnt,
sum(if(dog.pay_status >= 1 and dog.sku_shipping_status = 2, delivered_days, 0)) AS total_delivered_days,
sum(if(dog.pay_status >= 1 and dog.sku_shipping_status = 2 AND delivered_days is not null, 1, 0)) AS total_delivered_cnt,
count(distinct if(dog.pay_status >= 1  and dog.payment_id = 220, dog.order_goods_id, null)) AS paid_cod_order_goods_cnt,
sum(if(dog.pay_status >= 1 and dog.sku_shipping_status = 2 and dog.payment_id = 220, 1, 0)) AS receive_order_goods_cod_cnt,
sum(if(dog.pay_status >= 1 AND dog.container_transportation_shipping_fee_discount is not null, 1, 0)) AS discount_order_goods_cnt,
sum(if(dog.pay_status >= 1, dog.container_transportation_shipping_fee_discount, 0)) AS container_transportation_shipping_fee_discount,
sum(if(dog.pay_status >= 1, dog.container_transportation_shipping_fee + dog.shipping_fee, 0)) AS total_shipping_fee
from
(
select
dog.order_time,
dog.device_id,
dog.platform,
dog.mct_id,
case when
dog.mct_id in (26414, 11630, 36655) then '自营店铺'
else '第三方' end as mct_type,
dog.order_id,
dog.pay_status,
dog.payment_id,
dog.order_goods_id,
dog.buyer_id,
dog.shop_price,
dog.goods_number,
dog.shipping_fee,
if(fl.delivered_time is not null and dog.confirm_time is not null  ,round((unix_timestamp(fl.delivered_time) - unix_timestamp(dog.confirm_time)) / 3600 /24, 2),null ) as delivered_days,
dog.sku_shipping_status,
dog.container_transportation_shipping_fee,
temp_transportation.container_transportation_shipping_fee_discount
from
dim.dim_vova_order_goods dog
left join dwd.dwd_vova_fact_logistics fl on dog.order_goods_id = fl.order_goods_id
LEFT JOIN (
SELECT oget.rec_id,
       first(oget.extension_info) as container_transportation_shipping_fee_discount
       FROM ods_vova_vts.ods_vova_order_goods_extension oget
       WHERE oget.ext_name = 'container_transportation_shipping_fee_discount'
       group by oget.rec_id
) temp_transportation ON temp_transportation.rec_id = dog.order_goods_id
where
dog.region_code IN ('TW')
and date(dog.order_time) >= date_sub('${cur_date}', 30)
and date(dog.order_time) <= '${cur_date}'
and dog.sku_order_status != 5
) dog
left join dim.dim_vova_devices dd on dog.device_id = dd.device_id AND dd.datasource = 'vova'
GROUP BY CUBE (dog.platform, dog.mct_type, nvl(dd.child_channel, 'NA'), date(dog.order_time))
HAVING pt != 'all'
) final_data

;

INSERT OVERWRITE TABLE dwb.dwb_vova_tw_order_detail PARTITION (pt)
select
/*+ REPARTITION(1) */
date(oi.order_time) as event_date,
oi.order_sn,
oi.order_time,
sm.sm_desc as shipping_method_name,
case when oi.shipping_status = 0 then '未发货'
when oi.shipping_status = 2 then '已妥投'
else '未定义' end as shipping_status,
if(oext1.ext_value is null or oext1.ext_value = '', '宅配', '店配') AS shipping_type,
oi.payment_name,
oi.pay_time,
dog.order_goods_cnt,
dog.order_goods_sku_cnt,
oi.goods_amount + oi.shipping_fee as gmv,
oi.goods_amount,
oi.bonus,
oi.shipping_fee,
dog.container_transportation_shipping_fee,
nvl(oi.shipping_fee + dog.container_transportation_shipping_fee, 0) AS tot_shipping_fee,
dog.container_transportation_shipping_fee_discount,
date(oi.order_time) as pt
from
ods_vova_vts.ods_vova_order_info oi
left join ods_vova_vts.ods_vova_order_extension oext1 ON oi.order_id = oext1.order_id AND oext1.ext_name = 'store_address_id'
left join ods_vova_vts.ods_vova_shipping_method sm ON sm.sm_id = oi.sm_id
inner join (
select
dog.order_id,
count(dog.order_goods_id) AS order_goods_cnt,
sum(dog.goods_number) AS order_goods_sku_cnt,
sum(temp_transportation.container_transportation_shipping_fee_discount) AS container_transportation_shipping_fee_discount,
sum(dog.container_transportation_shipping_fee) AS container_transportation_shipping_fee
from
dim.dim_vova_order_goods dog
LEFT JOIN (
SELECT oget.rec_id,
       first(oget.extension_info) as container_transportation_shipping_fee_discount
       FROM ods_vova_vts.ods_vova_order_goods_extension oget
       WHERE oget.ext_name = 'container_transportation_shipping_fee_discount'
       group by oget.rec_id
) temp_transportation ON temp_transportation.rec_id = dog.order_goods_id
where dog.pay_status >= 1
and dog.region_code IN ('TW')
and date(dog.order_time) >= date_sub('${cur_date}', 30)
and date(dog.order_time) <= '${cur_date}'
and dog.sku_order_status != 5
group by dog.order_id
) dog on dog.order_id = oi.order_id

"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.maxExecutors=110" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=dwb_vova_tw_order" -e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi