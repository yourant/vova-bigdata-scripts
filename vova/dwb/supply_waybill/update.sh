#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "$cur_date"
#supply
sql="
-- waybill
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=1000;
with tmp_supply_waybill_detail as(
select
last_waybill_no,
carrier_code,
mct_id,
region_code,
confirm_date,
shop_price_amount,
shipping_fee,
bonus,
inbound_weight,
purchase_amount,
order_goods_cnt,
freight,
CASE
WHEN freight >0 AND freight <=5 THEN '(0,5]'
WHEN freight >5 AND freight <=10 THEN '(5,10]'
WHEN freight >10 AND freight <=15 THEN '(10,15]'
WHEN freight >15 AND freight <=20 THEN '(15,20]'
WHEN freight >20 AND freight <=30 THEN '(20,30]'
WHEN freight >30 AND freight <=50 THEN '(30,50]'
WHEN freight >50 AND freight <=100 THEN '(50,100]'
WHEN freight >100 THEN '100+'
ELSE freight END as freight_range,
CASE
WHEN weight >0 AND weight <=100 THEN '(0,100]'
WHEN weight >100 AND weight <=500 THEN '(100,500]'
WHEN weight >500 AND weight <=1000 THEN '(500,1000]'
WHEN weight >1000 AND weight <=2000 THEN '(1000,2000]'
WHEN weight >2000 AND weight <=3000 THEN '(2000,3000]'
WHEN weight >3000 AND weight <=4000 THEN '(3000,4000]'
WHEN weight >4000 THEN '(4000, +∞)'
ELSE weight END as weight_range
from
(
select
fspog.last_waybill_no,
fspog.carrier_code,
dog.mct_id,
dog.region_code,
date(dog.confirm_time) as confirm_date,
sum(dog.shop_price * dog.goods_number) as shop_price_amount,
sum(dog.shipping_fee) as shipping_fee,
sum(dog.bonus) as bonus,
sum(fspog.inbound_weight) as inbound_weight,
sum(if(fspog.purchase_order_status = 5 , fspog.purchase_amount, 0)) as purchase_amount,
if(first(actual_freight) != 0, first(actual_freight), first(plan_freight)) AS freight,
if(first(actual_weight) != 0, first(actual_weight), first(guess_weight)) AS weight,
count(distinct dog.order_goods_id) AS order_goods_cnt
from
dim.dim_vova_order_goods dog
inner join dwd.dwd_vova_fact_supply_order_goods fspog on fspog.channel_order_goods_sn = dog.order_goods_sn
where dog.pay_status >= 1
and dog.sku_order_status != 5
and dog.mct_id IN (26414, 11630, 36655,61017,61028,61235,61310)
and date(dog.confirm_time) = '${cur_date}'
and fspog.refer_waybill_no is not null
GROUP BY fspog.last_waybill_no, fspog.carrier_code, dog.mct_id, dog.region_code, date(dog.confirm_time)
) t1
)
INSERT overwrite TABLE dwb.dwb_vova_supply_waybill PARTITION (pt='${cur_date}')
select
t1.event_date,
mct_name,
region_code,
carrier_code,
freight_range,
weight_range,
waybill_cnt,
order_goods_cnt,
gmv,
order_amount,
purchase_amount,
shipping_fee,
freight,
profit1, --币种
profit2,
shipping_profit,
avg_profit1,
avg_profit2,
avg_shipping_profit,
tmp.tot_waybill_cnt,
nvl(waybill_cnt / tmp.tot_waybill_cnt, 0) AS waybill_rate
from
(
select
'${cur_date}' AS event_date,
nvl(dm.mct_name, 'all') AS mct_name,
nvl(region_code, 'all') AS region_code,
nvl(carrier_code, 'all') AS carrier_code,
nvl(freight_range, 'all') AS freight_range,
nvl(weight_range, 'all') AS weight_range,
count(last_waybill_no) AS waybill_cnt,
sum(order_goods_cnt) AS order_goods_cnt,
sum(shop_price_amount + shipping_fee) AS gmv,
sum(shop_price_amount + shipping_fee + bonus) AS order_amount,
sum(purchase_amount / 6.9) AS purchase_amount,
sum(shipping_fee) AS shipping_fee,
sum(freight / 6.9) AS freight,
sum(shop_price_amount + shipping_fee) - (sum(freight + purchase_amount) + 3 * sum(order_goods_cnt)) / 6.9  AS profit1, --币种
sum(shop_price_amount + shipping_fee + bonus) - (sum(freight + purchase_amount) + 3 * sum(order_goods_cnt)) / 6.9 AS profit2,
sum(shipping_fee) - (sum(freight) + 3 * sum(order_goods_cnt)) / 6.9 AS shipping_profit,
(sum(shop_price_amount + shipping_fee) - (sum(freight + purchase_amount) + 3 * sum(order_goods_cnt)) / 6.9) / count(last_waybill_no) AS avg_profit1,
(sum(shop_price_amount + shipping_fee + bonus) - (sum(freight + purchase_amount) + 3 * sum(order_goods_cnt)) / 6.9) / count(last_waybill_no) AS avg_profit2,
(sum(shipping_fee) - (sum(freight) + 3 * sum(order_goods_cnt)) / 6.9) / count(last_waybill_no) AS avg_shipping_profit
from tmp_supply_waybill_detail swd
inner join dim.dim_vova_merchant dm on dm.mct_id = swd.mct_id
WHERE swd.confirm_date = '${cur_date}'
GROUP BY cube (
dm.mct_name,
region_code,
carrier_code,
freight_range,
weight_range
)
) t1
LEFT JOIN
(
SELECT
'${cur_date}' AS event_date,
count(distinct last_waybill_no) AS tot_waybill_cnt
FROM
tmp_supply_waybill_detail swd
where swd.confirm_date = '${cur_date}'
) tmp on tmp.event_date = t1.event_date

;


-- order
insert overwrite table tmp.tmp_supply_waybill_order_detail
select
order_goods_id,
goods_id,
virtual_goods_id,
carrier_code,
mct_id,
region_code,
confirm_date,
first_cat_name,
second_cat_name,
three_cat_name,
CASE
WHEN order_goods_amount >0 AND order_goods_amount <=5 THEN '(0,5]'
WHEN order_goods_amount >5 AND order_goods_amount <=10 THEN '(5,10]'
WHEN order_goods_amount >10 AND order_goods_amount <=15 THEN '(10,15]'
WHEN order_goods_amount >15 AND order_goods_amount <=20 THEN '(15,20]'
WHEN order_goods_amount >20 AND order_goods_amount <=30 THEN '(20,30]'
WHEN order_goods_amount >30 AND order_goods_amount <=50 THEN '(30,50]'
WHEN order_goods_amount >50 AND order_goods_amount <=100 THEN '(50,100]'
WHEN order_goods_amount >100 THEN '100+'
ELSE order_goods_amount END as order_goods_amount_range,
CASE
WHEN goods_amount >0 AND goods_amount <=5 THEN '(0,5]'
WHEN goods_amount >5 AND goods_amount <=10 THEN '(5,10]'
WHEN goods_amount >10 AND goods_amount <=15 THEN '(10,15]'
WHEN goods_amount >15 AND goods_amount <=20 THEN '(15,20]'
WHEN goods_amount >20 AND goods_amount <=30 THEN '(20,30]'
WHEN goods_amount >30 AND goods_amount <=50 THEN '(30,50]'
WHEN goods_amount >50 AND goods_amount <=100 THEN '(50,100]'
WHEN goods_amount >100 THEN '100+'
ELSE goods_amount END as goods_amount_range,
CASE
WHEN weight >0 AND weight <=100 THEN '(0,100]'
WHEN weight >100 AND weight <=500 THEN '(100,500]'
WHEN weight >500 AND weight <=1000 THEN '(500,1000]'
WHEN weight >1000 AND weight <=2000 THEN '(1000,2000]'
WHEN weight >2000 AND weight <=3000 THEN '(2000,3000]'
WHEN weight >3000 AND weight <=4000 THEN '(3000,4000]'
WHEN weight >4000 THEN '(4000, +∞)'
ELSE weight END as weight_range,
purchase_amount,
if(purchase_amount > 0 , 'Y', 'N') AS valid_purchase,
if(shipping_loss_amount > 0 , 'Y', 'N') AS loss_order,
CASE
WHEN shipping_loss_amount >0 AND shipping_loss_amount <=1 THEN '(0,1]'
WHEN shipping_loss_amount >1 AND shipping_loss_amount <=3 THEN '(1,3]'
WHEN shipping_loss_amount >3 AND shipping_loss_amount <=5 THEN '(3,5]'
WHEN shipping_loss_amount >5 AND shipping_loss_amount <=10 THEN '(5,10]'
WHEN shipping_loss_amount >10 THEN '10+'
ELSE shipping_loss_amount END as shipping_loss_amount_range,
shop_price,
goods_number,
shipping_fee,
bonus,
freight
from
(
select
dog.order_goods_id,
dog.goods_id,
dog.virtual_goods_id,
fspog.carrier_code,dwd_vova_fact_supply_order_goods
dog.mct_id,
dog.region_code,
date(dog.confirm_time) as confirm_date,
fspog.inbound_weight / tot_waybill.inbound_weight * tot_waybill.freight  AS freight,
fspog.inbound_weight / tot_waybill.inbound_weight * tot_waybill.weight  AS weight,
dog.shop_price * dog.goods_number + dog.shipping_fee + dog.bonus as order_goods_amount,
dg.shop_price + dg.shipping_fee AS goods_amount,
nvl(nvl(fspog.inbound_weight / tot_waybill.inbound_weight * tot_waybill.freight, 0) / 6.9 - dog.shipping_fee, 0) AS shipping_loss_amount,
sg.first_cat_name,
sg.second_cat_name,
sg.three_cat_name,
if(fspog.purchase_order_status = 5 , fspog.purchase_amount, 0) AS purchase_amount,
dog.shop_price,
dog.goods_number,
dog.shipping_fee,
dog.bonus
from
dim.dim_vova_order_goods dog
inner join dim.dim_vova_goods dg on dg.goods_id = dog.goods_id
inner join dwd.dwd_vova_fact_supply_order_goods fspog on fspog.channel_order_goods_sn = dog.order_goods_sn
left join dim.dim_vova_supply_goods sg on sg.goods_id = dog.goods_id
left join
(
select
last_waybill_no,
sum(inbound_weight) as inbound_weight,
if(first(actual_freight) != 0, first(actual_freight), first(plan_freight)) AS freight,
if(first(actual_weight) != 0, first(actual_weight), first(guess_weight)) AS weight
from dwd.dwd_vova_fact_supply_order_goods
group by last_waybill_no
) tot_waybill ON tot_waybill.last_waybill_no = fspog.last_waybill_no
where dog.pay_status >= 1
and dog.sku_order_status != 5
and dog.mct_id IN (26414, 11630, 36655,61017,61028,61235,61310)
and date(dog.confirm_time) = '${cur_date}'
and fspog.refer_waybill_no is not null
) t1;

INSERT overwrite TABLE dwb.dwb_vova_supply_waybill_order PARTITION (pt='${cur_date}')
select
t1.event_date,
mct_name,
region_code,
carrier_code,
order_goods_amount_range,
goods_amount_range,
weight_range,
first_cat_name,
second_cat_name,
three_cat_name,
valid_purchase,
order_goods_cnt,
gmv,
order_amount,
purchase_amount,
shipping_fee,
freight,
profit1, --币种
profit2,
shipping_profit,
avg_profit1,
avg_profit2,
avg_shipping_profit,
tot_order_goods_cnt,
nvl(order_goods_cnt / tot_order_goods_cnt, 0) AS order_goods_rate
from
(
select
'${cur_date}' AS event_date,
nvl(dm.mct_name, 'all') AS mct_name,
nvl(swod.region_code, 'all') AS region_code,
nvl(swod.carrier_code, 'all') AS carrier_code,
nvl(swod.order_goods_amount_range, 'all') AS order_goods_amount_range,
nvl(swod.goods_amount_range, 'all') AS goods_amount_range,
nvl(nvl(swod.weight_range, 'NALL'), 'all') AS weight_range,
nvl(nvl(swod.first_cat_name, 'NALL'), 'all') AS first_cat_name,
nvl(nvl(swod.second_cat_name, 'NALL'), 'all') AS second_cat_name,
nvl(nvl(swod.three_cat_name, 'NALL'), 'all') AS three_cat_name,
nvl(swod.valid_purchase, 'all') AS valid_purchase,
count(swod.order_goods_id) AS order_goods_cnt,
sum(shop_price * goods_number + shipping_fee) AS gmv,
sum(shop_price * goods_number + shipping_fee + bonus) AS order_amount,
sum(purchase_amount / 6.9) AS purchase_amount,
sum(shipping_fee) AS shipping_fee,
sum(freight / 6.9) AS freight,
sum(shop_price * goods_number + shipping_fee) - (sum(freight + purchase_amount) + 3) / 6.9 AS profit1, --币种
sum(shop_price * goods_number + shipping_fee + bonus) - (sum(freight + purchase_amount) + 3) / 6.9  AS profit2,
sum(shipping_fee) - (sum(freight) + 3) / 6.9 AS shipping_profit,
(sum(shop_price * goods_number + shipping_fee) - (sum(freight + purchase_amount) + 3) / 6.9) / count(swod.order_goods_id) AS avg_profit1,
(sum(shop_price * goods_number + shipping_fee + bonus) - (sum(freight + purchase_amount) + 3) / 6.9) / count(swod.order_goods_id) AS avg_profit2,
(sum(shipping_fee) - (sum(freight) + 3) / 6.9) / count(swod.order_goods_id) AS avg_shipping_profit
from tmp.tmp_supply_waybill_order_detail swod
inner join dim.dim_vova_merchant dm on dm.mct_id = swod.mct_id
WHERE swod.confirm_date = '${cur_date}'
GROUP BY CUBE
(
dm.mct_name,
swod.region_code,
swod.carrier_code,
swod.order_goods_amount_range,
swod.goods_amount_range,
nvl(swod.weight_range, 'NALL'),
nvl(swod.first_cat_name, 'NALL'),
nvl(swod.second_cat_name, 'NALL'),
nvl(swod.three_cat_name, 'NALL'),
swod.valid_purchase
)
) t1
LEFT JOIN
(
SELECT
'${cur_date}' AS event_date,
count(distinct order_goods_id) AS tot_order_goods_cnt
FROM
tmp.tmp_supply_waybill_order_detail swod
where swod.confirm_date = '${cur_date}'
) tmp on tmp.event_date = t1.event_date
;

INSERT overwrite TABLE dwb.dwb_vova_supply_waybill_order_loss PARTITION (pt='${cur_date}')
select
t1.event_date,
mct_name,
region_code,
carrier_code,
order_goods_amount_range,
goods_amount_range,
weight_range,
first_cat_name,
second_cat_name,
three_cat_name,
valid_purchase,
shipping_loss_amount_range,
order_goods_cnt,
gmv,
order_amount,
purchase_amount,
shipping_fee,
freight,
profit1, --币种
profit2,
shipping_profit,
avg_profit1,
avg_profit2,
avg_shipping_profit,
tot_order_goods_cnt,
nvl(order_goods_cnt / tot_order_goods_cnt, 0) AS order_goods_rate
from
(
select
'${cur_date}' AS event_date,
nvl(dm.mct_name, 'all') AS mct_name,
nvl(swod.region_code, 'all') AS region_code,
nvl(swod.carrier_code, 'all') AS carrier_code,
nvl(swod.order_goods_amount_range, 'all') AS order_goods_amount_range,
nvl(swod.goods_amount_range, 'all') AS goods_amount_range,
nvl(nvl(swod.weight_range, 'NALL'), 'all') AS weight_range,
nvl(nvl(swod.first_cat_name, 'NALL'), 'all') AS first_cat_name,
nvl(nvl(swod.second_cat_name, 'NALL'), 'all') AS second_cat_name,
nvl(nvl(swod.three_cat_name, 'NALL'), 'all') AS three_cat_name,
nvl(swod.valid_purchase, 'all') AS valid_purchase,
nvl(swod.shipping_loss_amount_range, 'all') AS shipping_loss_amount_range,
count(swod.order_goods_id) AS order_goods_cnt,
sum(shop_price * goods_number + shipping_fee) AS gmv,
sum(shop_price * goods_number + shipping_fee + bonus) AS order_amount,
sum(purchase_amount / 6.9) AS purchase_amount,
sum(shipping_fee) AS shipping_fee,
sum(freight / 6.9) AS freight,
sum(shop_price * goods_number + shipping_fee) - (sum(freight + purchase_amount) + 3) / 6.9 AS profit1, --币种
sum(shop_price * goods_number + shipping_fee + bonus) - (sum(freight + purchase_amount) + 3) / 6.9  AS profit2,
sum(shipping_fee) - (sum(freight) + 3) / 6.9 AS shipping_profit,
(sum(shop_price * goods_number + shipping_fee) - (sum(freight + purchase_amount) + 3) / 6.9) / count(swod.order_goods_id) AS avg_profit1,
(sum(shop_price * goods_number + shipping_fee + bonus) - (sum(freight + purchase_amount) + 3) / 6.9) / count(swod.order_goods_id) AS avg_profit2,
(sum(shipping_fee) - (sum(freight) + 3) / 6.9) / count(swod.order_goods_id) AS avg_shipping_profit
from tmp.tmp_supply_waybill_order_detail swod
inner join dim.dim_vova_merchant dm on dm.mct_id = swod.mct_id
WHERE swod.confirm_date = '${cur_date}'
AND swod.loss_order = 'Y'
GROUP BY CUBE
(
dm.mct_name,
swod.region_code,
swod.carrier_code,
swod.order_goods_amount_range,
swod.goods_amount_range,
nvl(swod.weight_range, 'NALL'),
nvl(swod.first_cat_name, 'NALL'),
nvl(swod.second_cat_name, 'NALL'),
nvl(swod.three_cat_name, 'NALL'),
swod.valid_purchase,
swod.shipping_loss_amount_range
)
) t1
LEFT JOIN
(
SELECT
'${cur_date}' AS event_date,
count(distinct order_goods_id) AS tot_order_goods_cnt
FROM
tmp.tmp_supply_waybill_order_detail swod
where swod.confirm_date = '${cur_date}'
AND swod.loss_order = 'Y'
) tmp on tmp.event_date = t1.event_date
;
"

#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=dwb_vova_supply_waybill_order_loss" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
--conf "spark.sql.codegen.wholeStage=false" \
-e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi