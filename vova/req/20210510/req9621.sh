9621 【取数需求】拉取商详页立即购买新流程数据
"A组和B组的区别是testinfo字段中：
ab_9332_pay_now_a是A组；
ab_9332_pay_now_b是B组；

A组“立即购买”浮层曝光的用户为有“商详页立即购买”按钮点击的A组用户，其中“商详页立即购买”按钮的点击“APP埋点汇总”中标黄的部分
B组“立即购买”浮层曝光的用户为有“信用卡快捷支付提示”曝光的B组用户，其中信用卡快捷支付曝光的打点参见打点文档中标黄的部分"

AND log.test_info like '%ab_9332_pay_now%'


#start
#step1
select
log.pt,
'A' AS test_info2,
count(distinct device_id) AS confirm_uv
from
dwd.dwd_vova_log_click_arc log
WHERE log.datasource = 'vova'
AND log.platform = 'mob'
AND log.pt >= '2021-05-28'
AND log.pt <= '2021-05-30'
AND log.event_type='normal'
AND log.element_name='buy_now_at_product_options_dialog'
group by log.pt

UNION

select
log.pt,
'B' AS test_info2,
count(distinct device_id) AS confirm_uv
from
dwd.dwd_vova_log_impressions_arc log
WHERE log.datasource = 'vova'
AND log.pt >= '2021-05-28'
AND log.pt <= '2021-05-30'
AND log.event_type='normal'
AND log.element_name='FastPaymentTip'
group by log.pt
;

#step2
drop table if exists tmp.tmp_zyzheng_req_base_9621;
create table tmp.tmp_zyzheng_req_base_9621 as
select
log.pt,
log.device_id,
log.test_info,
log.extra,
get_json_object(extra, '$.element_content') AS element_content,
get_json_object(extra, '$.fastpayment') AS fastpayment,
get_json_object(extra, '$.paymentmethod') AS paymentmethod
from
dwd.dwd_vova_log_data log
WHERE log.datasource = 'vova'
AND log.platform = 'mob'
AND log.pt >= '2021-05-19'
AND log.pt <= '2021-05-30'
AND log.test_info like '%ab_9332_pay_now%'
AND log.element_name = 'order_placed'
;

--paymentmethod=credit/other
--select distinct paymentmethod from  tmp.tmp_zyzheng_req_base_9621 ;
select * from tmp.tmp_zyzheng_req_base_9621 where paymentmethod IN ('credit', 'other') limit 10;
select * from tmp.tmp_zyzheng_req_base_9621_2 where el_type IN ('B1', 'B2') limit 10;


drop table if exists tmp.tmp_zyzheng_req_9621;
create table tmp.tmp_zyzheng_req_9621 as
select
log.pt,
log.device_id,
case when log.test_info like '%ab_9332_pay_now_a%' then 'A'
     when log.test_info like '%ab_9332_pay_now_b%' AND paymentmethod = 'credit' then 'B1'
     when log.test_info like '%ab_9332_pay_now_b%' AND paymentmethod = 'other' then 'B2'
     else 'others' end AS el_type,
count(*) AS pv
from
tmp.tmp_zyzheng_req_base_9621 log
group by log.pt,
log.device_id,
case when log.test_info like '%ab_9332_pay_now_a%' then 'A'
     when log.test_info like '%ab_9332_pay_now_b%' AND paymentmethod = 'credit' then 'B1'
     when log.test_info like '%ab_9332_pay_now_b%' AND paymentmethod = 'other' then 'B2'
     else 'others' end
;

select el_type,count(*),count(distinct pt,device_id) from tmp.tmp_zyzheng_req_9621 where el_type in ('A', 'B1', 'B2') group by el_type;
select distinct order_sn from tmp.tmp_zyzheng_req_9621 where el_type in ('A', 'B1', 'B2') group by el_type;

select
base.pt,
base.el_type,
count(distinct base.device_id) AS order_user_uv,
count(distinct oi.order_sn) AS order_cnt,
count(oi.order_sn) AS order_cnt,
count(distinct if(oi.pay_status >= 1, oi.order_sn, null)) AS paid_success_order_cnt,
count(distinct fr.order_id) AS cancel,
count(distinct fr2.order_id) AS refund,
round(sum(if(oi.pay_status >= 1, oi.goods_amount + oi.shipping_fee, 0)), 2) as gmv,
round(sum(if(fr.order_id is not null, fr.gmv, 0)), 2) as gmv2,
round(sum(if(fr2.order_id is not null, fr2.gmv, 0)), 2) as gmv3
from
tmp.tmp_zyzheng_req_9621 base
INNER JOIN (
select
oi.order_time,
ore.device_id,
oi.order_id,
oi.order_sn,
oi.pay_status,
oi.goods_amount,
oi.shipping_fee
from
ods_vova_vts.ods_vova_order_info oi
inner JOIN ods_vova_vts.ods_vova_order_relation ore ON ore.order_id = oi.order_id
where oi.parent_order_id = 0
and oi.project_name = 'vova'
) oi on oi.device_id = base.device_id and date(oi.order_time) = base.pt
LEFT JOIN (
SELECT
og.order_id,
sum(og.shop_price * og.goods_number + og.shipping_fee) AS gmv
FROM
dwd.dwd_vova_fact_refund fr
inner join ods_vova_vts.ods_vova_order_goods og on og.rec_id = fr.order_goods_id
inner join ods_vova_vts.ods_vova_order_goods_status ogs on ogs.order_goods_id = fr.order_goods_id
where ogs.sku_order_status = 2
AND ogs.sku_pay_status>= 1
AND fr.refund_type_id != 2
group by og.order_id
) fr on fr.order_id = oi.order_id
LEFT JOIN (
SELECT
og.order_id,
sum(og.shop_price * og.goods_number + og.shipping_fee) AS gmv
FROM
dwd.dwd_vova_fact_refund fr
inner join ods_vova_vts.ods_vova_order_goods og on og.rec_id = fr.order_goods_id
inner join ods_vova_vts.ods_vova_order_goods_status ogs on ogs.order_goods_id = fr.order_goods_id
where fr.refund_type_id = 2
AND ogs.sku_pay_status>= 1
group by og.order_id
) fr2 on fr2.order_id = oi.order_id
where base.el_type != 'others'
group by
base.pt
, base.el_type
;


select
base.pt,
p.payment_code,
count(*),
count(distinct oi.order_sn)
from
tmp.tmp_zyzheng_req_9621 base
INNER JOIN (
select
oi.order_time,
ore.device_id,
oi.order_id,
oi.order_sn,
oi.pay_status,
oi.goods_amount,
oi.payment_id,
oi.shipping_fee
from
ods_vova_vts.ods_vova_order_info oi
inner JOIN ods_vova_vts.ods_vova_order_relation ore ON ore.order_id = oi.order_id
where oi.parent_order_id = 0
and oi.pay_status >=1
and oi.project_name = 'vova'
) oi on oi.device_id = base.device_id and date(oi.order_time) = base.pt
left join dim.dim_vova_payment p on p.payment_id = oi.payment_id
where base.el_type IN ('B2')
group by base.pt, p.payment_code

#end


