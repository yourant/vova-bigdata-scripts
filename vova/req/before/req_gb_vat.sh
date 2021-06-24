select
count(*),count(distinct oi.order_id),count(og.rec_id),
payment_id
from
ods_vova_vts.ods_vova_order_info oi
inner join ods_vova_vts.ods_vova_order_goods og on oi.order_id = og.order_id
LEFT JOIN ods_vova_vts.ods_vova_region r on r.region_id = oi.country
WHERE oi.pay_status = 2
AND date(oi.receive_time) >= '2021-01-01'
AND date(oi.receive_time) < '2021-04-01'
AND oi.country = 3858
AND oi.parent_order_id = 0
group by payment_id
;

spark-sql -e "
select
oi.order_sn,
og.order_goods_sn,
og.rec_id AS order_goods_id,
g.mct_id,
'Sale' AS event_type,
oi.receive_time,
r.region_code,
regexp_replace(oi.consignee,'\n|\t|\r', ' ') AS consignee,
regexp_replace(oi.address,'\n|\t|\r', ' ') AS address,
regexp_replace(og.goods_name,'\n|\t|\r', ' ') AS goods_name,
og.goods_number,
c.currency,
og.shop_price_exchange  / (1 + nvl(ge.extension_info, 0)),
og.shop_price_exchange * og.goods_number / (1 + nvl(ge.extension_info, 0)),
(og.shipping_fee_exchange - nvl(ge2.extension_info, 0)) / (1 + nvl(ge.extension_info, 0)) + nvl(ge2.extension_info, 0),
og.bonus_exchange,
og.shop_price_exchange * og.goods_number / (1 + nvl(ge.extension_info, 0)) + (og.shipping_fee_exchange - nvl(ge2.extension_info, 0)) / (1 + nvl(ge.extension_info, 0)) + nvl(ge2.extension_info, 0) + og.bonus_exchange,
'20%' AS vat_rate,
og.shop_price_exchange * og.goods_number * nvl(ge.extension_info, 0) + (og.shipping_fee_exchange - nvl(ge2.extension_info, 0)) * nvl(ge.extension_info, 0),
og.shop_price_exchange * og.goods_number + og.shipping_fee_exchange + og.bonus_exchange,
nvl(ge.extension_info, 0),
nvl(ge2.extension_info, 0)
from
ods_vova_vts.ods_vova_order_info oi
inner join ods_vova_vts.ods_vova_order_goods og on oi.order_id = og.order_id
inner join dim.dim_vova_goods g on g.goods_id = og.goods_id
LEFT JOIN ods_vova_vts.ods_vova_region r on r.region_id = oi.country
LEFT JOIN ods_vova_vts.ods_vova_currency c on c.currency_id = oi.order_currency_id
LEFT JOIN (SELECT ge.rec_id, if(ge.extension_info is null, 0, ge.extension_info -1)AS extension_info FROM ods_vova_vts.ods_vova_order_goods_extension ge WHERE ge.ext_name = 'vat_config_ratio') ge on ge.rec_id = og.rec_id
LEFT JOIN (SELECT ge.rec_id, nvl(ge.extension_info, 0) AS extension_info FROM ods_vova_vts.ods_vova_order_goods_extension ge WHERE ge.ext_name = 'container_transportation_shipping_fee_exchange') ge2 on ge2.rec_id = og.rec_id
WHERE oi.pay_status = 2
AND date(oi.receive_time) >= '2021-01-01'
AND date(oi.receive_time) < '2021-04-01'
AND oi.country = 3858
AND oi.email NOT REGEXP '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
AND oi.parent_order_id = 0
AND oi.project_name = 'vova'
;
"  > gb_receive_20210409.csv

spark-sql -e "
select
oi.order_sn,
og.order_goods_sn,
og.rec_id AS order_goods_id,
g.mct_id,
'Refund' AS event_type,
fr.exec_refund_time,
r.region_code,
regexp_replace(oi.consignee,'\n|\t|\r', ' ') AS consignee,
regexp_replace(oi.address,'\n|\t|\r', ' ') AS address,
regexp_replace(og.goods_name,'\n|\t|\r', ' ') AS goods_name,
og.goods_number,
c.currency,
og.shop_price_exchange  / (1 + nvl(ge.extension_info, 0)),
og.shop_price_exchange * og.goods_number / (1 + nvl(ge.extension_info, 0)),
(og.shipping_fee_exchange - nvl(ge2.extension_info, 0)) / (1 + nvl(ge.extension_info, 0)) + nvl(ge2.extension_info, 0),
og.bonus_exchange,
og.shop_price_exchange * og.goods_number / (1 + nvl(ge.extension_info, 0)) + (og.shipping_fee_exchange - nvl(ge2.extension_info, 0)) / (1 + nvl(ge.extension_info, 0)) + nvl(ge2.extension_info, 0) + og.bonus_exchange,
'20%' AS vat_rate,
og.shop_price_exchange * og.goods_number * nvl(ge.extension_info, 0) + (og.shipping_fee_exchange - nvl(ge2.extension_info, 0)) * nvl(ge.extension_info, 0),
fr.refund_amount_exchange,
nvl(ge.extension_info, 0),
nvl(ge2.extension_info, 0)
from
dwd.dwd_vova_fact_refund fr
inner join dim.dim_vova_order_goods dog on fr.order_goods_id = dog.order_goods_id
inner join ods_vova_vts.ods_vova_order_goods og on fr.order_goods_id = og.rec_id
inner join ods_vova_vts.ods_vova_order_info oi on oi.order_id = og.order_id
inner join dim.dim_vova_goods g on g.goods_id = og.goods_id
LEFT JOIN ods_vova_vts.ods_vova_region r on r.region_id = oi.country
LEFT JOIN ods_vova_vts.ods_vova_currency c on c.currency_id = oi.order_currency_id
LEFT JOIN (SELECT ge.rec_id, if(ge.extension_info is null, 0, ge.extension_info -1) AS extension_info FROM ods_vova_vts.ods_vova_order_goods_extension ge WHERE ge.ext_name = 'vat_config_ratio') ge on ge.rec_id = og.rec_id
LEFT JOIN (SELECT ge.rec_id, nvl(ge.extension_info, 0) AS extension_info FROM ods_vova_vts.ods_vova_order_goods_extension ge WHERE ge.ext_name = 'container_transportation_shipping_fee_exchange') ge2 on ge2.rec_id = og.rec_id
WHERE dog.sku_pay_status = 4
AND date(fr.exec_refund_time) >= '2021-01-01'
AND date(fr.exec_refund_time) < '2021-04-01'
AND oi.country = 3858
AND oi.project_name = 'vova'
;
"  > gb_refund_20210409.csv

select count(*),count(distinct fr.order_goods_id)
from
dwd.dwd_vova_fact_refund fr
inner join dim.dim_vova_order_goods dog on fr.order_goods_id = dog.order_goods_id
inner join ods_vova_vts.ods_vova_order_goods og on fr.order_goods_id = og.rec_id
inner join ods_vova_vts.ods_vova_order_info oi on oi.order_id = og.order_id
inner join dim.dim_vova_goods g on g.goods_id = og.goods_id
LEFT JOIN ods_vova_vts.ods_vova_region r on r.region_id = oi.country
LEFT JOIN ods_vova_vts.ods_vova_currency c on c.currency_id = oi.order_currency_id
LEFT JOIN (SELECT ge.rec_id, nvl(nvl(ge.extension_info, 0), 0) AS extension_info FROM ods_vova_vts.ods_vova_order_goods_extension ge WHERE ge.ext_name = 'vat_config_ratio') ge on ge.rec_id = og.rec_id
LEFT JOIN (SELECT ge.rec_id, nvl(nvl(ge.extension_info, 0), 0) AS extension_info FROM ods_vova_vts.ods_vova_order_goods_extension ge WHERE ge.ext_name = 'container_transportation_shipping_fee_exchange') ge2 on ge2.rec_id = og.rec_id
WHERE dog.sku_pay_status = 4
AND date(fr.exec_refund_time) >= '2021-01-01'
AND date(fr.exec_refund_time) < '2021-04-01'



select
DISTINCT oi.email
from
ods_vova_vts.ods_vova_order_info oi
inner join ods_vova_vts.ods_vova_order_goods og on oi.order_id = og.order_id
inner join dim.dim_vova_goods g on g.goods_id = og.goods_id
LEFT JOIN ods_vova_vts.ods_vova_region r on r.region_id = oi.country
LEFT JOIN ods_vova_vts.ods_vova_currency c on c.currency_id = oi.order_currency_id
LEFT JOIN ods_vova_vts.ods_vova_order_goods_extension ge on ge.rec_id = og.rec_id AND ge.ext_name = 'vat_config_ratio'
LEFT JOIN ods_vova_vts.ods_vova_order_goods_extension ge2 on ge2.rec_id = og.rec_id AND ge.ext_name = 'container_transportation_shipping_fee'
WHERE oi.pay_status = 2
AND date(oi.receive_time) >= '2021-01-01'
AND date(oi.receive_time) < '2021-04-01'
AND oi.email REGEXP '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
AND oi.country = 3858
AND oi.parent_order_id = 0
;


