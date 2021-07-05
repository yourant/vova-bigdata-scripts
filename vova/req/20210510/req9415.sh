"A组和B组的区别是testinfo字段中：
ab_9091_creditcard_fast_payment_a是A组；
ab_9091_creditcard_fast_payment_b是B组；

信用卡快捷支付曝光的打点参见打点文档"
;





#start
#step1
select
log.pt,
case when log.test_info like '%ab_9091_creditcard_fast_payment_b%' then 'B'
     when log.test_info like '%ab_9091_creditcard_fast_payment_a%' then 'A'
     else log.test_info end as test_info2,
count(distinct device_id) AS confirm_uv
from
dwd.dwd_vova_log_impressions_arc log
WHERE log.datasource = 'vova'
AND log.platform = 'mob'
AND log.pt >= '2021-05-28'
AND log.pt <= '2021-05-30'
AND log.event_type='normal'
AND log.test_info like '%ab_9091_creditcard_fast_payment%'
group by log.pt,
case when log.test_info like '%ab_9091_creditcard_fast_payment_b%' then 'B'
     when log.test_info like '%ab_9091_creditcard_fast_payment_a%' then 'A'
     else log.test_info end
;

#step2
drop table if exists tmp.tmp_zyzheng_req_base_9415;
create table tmp.tmp_zyzheng_req_base_9415 as
select
log.pt,
log.device_id,
log.test_info,
log.extra,
get_json_object(extra, '$.element_content') AS element_content,
get_json_object(extra, '$.fastpayment') AS fastpayment
from
dwd.dwd_vova_log_data log
WHERE log.datasource = 'vova'
AND log.platform = 'mob'
AND log.pt >= '2021-05-19'
AND log.pt <= '2021-05-30'
AND log.test_info like '%ab_9091_creditcard_fast_payment%'
AND log.element_name = 'order_placed'
;

drop table if exists tmp.tmp_zyzheng_req_base_9415_2;
create table tmp.tmp_zyzheng_req_base_9415_2 as
select
log.pt,
log.device_id,
log.element_content AS order_sn,
case when log.test_info like '%ab_9091_creditcard_fast_payment_a%' then 'A'
     when log.test_info like '%ab_9091_creditcard_fast_payment_b%' AND fastpayment = 'on' then 'B1'
     when log.test_info like '%ab_9091_creditcard_fast_payment_b%' AND fastpayment = 'off' then 'B2'
     else 'others' end AS el_type,
count(*) AS pv
from
tmp.tmp_zyzheng_req_base_9415 log
group by log.pt,
log.device_id,
log.element_content,
case when log.test_info like '%ab_9091_creditcard_fast_payment_a%' then 'A'
     when log.test_info like '%ab_9091_creditcard_fast_payment_b%' AND fastpayment = 'on' then 'B1'
     when log.test_info like '%ab_9091_creditcard_fast_payment_b%' AND fastpayment = 'off' then 'B2'
     else 'others' end
;

select
base.pt,
base.el_type,
count(distinct base.device_id) AS order_user_uv,
count(distinct base.order_sn) AS order_cnt,
count(base.order_sn) AS order_cnt,
count(distinct if(oi.pay_status >= 1, oi.order_sn, null)) AS paid_success_order_cnt,
count(distinct fr.order_id) AS cancel,
count(distinct fr2.order_id) AS refund,
round(sum(if(oi.pay_status >= 1, oi.goods_amount + oi.shipping_fee, 0)), 2) as gmv,
round(sum(if(fr.order_id is not null, fr.gmv, 0)), 2) as gmv2,
round(sum(if(fr2.order_id is not null, fr2.gmv, 0)), 2) as gmv3
from
tmp.tmp_zyzheng_req_base_9415_2 base
INNER JOIN ods_vova_vts.ods_vova_order_info oi on oi.order_sn = base.order_sn
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
log.pt,
p.payment_code,
count(*),
count(distinct log.order_sn)
from
(
select
log.pt,
log.order_sn
from
tmp.tmp_zyzheng_req_base_9415_2 log
where log.el_type in ('B2')
group by log.pt, log.order_sn
) log
inner join ods_vova_vts.ods_vova_order_info oi on oi.order_sn = log.order_sn
left join dim.dim_vova_payment p on p.payment_id = oi.payment_id
where oi.project_name = 'vova'
and oi.pay_status >= 1
group by log.pt, p.payment_code


#end




select pt,count(*),count(distinct device_id,fastpayment,test_info) from tmp.tmp_zyzheng_req_base_9415 group by pt order  by pt;
select * from tmp.tmp_zyzheng_req_base_9415_2 order by pv desc limit 10;
select * from tmp.tmp_zyzheng_req_base_9415_2 where el_type = 'others';
select * from tmp.tmp_zyzheng_req_base_9415 where element_content = '0416cb7f5cffe62c';
select pt,count(*),count(distinct order_sn) from tmp.tmp_zyzheng_req_base_9415_2 where el_type != 'others' group by pt order  by pt;
;
select
fr2.*
from
tmp.tmp_zyzheng_req_base_9415_2 base
INNER JOIN ods_vova_vts.ods_vova_order_info oi on oi.order_sn = base.order_sn
LEFT JOIN (
SELECT
og.order_id
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
og.order_id
FROM
dwd.dwd_vova_fact_refund fr
inner join ods_vova_vts.ods_vova_order_goods og on og.rec_id = fr.order_goods_id
inner join ods_vova_vts.ods_vova_order_goods_status ogs on ogs.order_goods_id = fr.order_goods_id
where fr.refund_type_id = 2
group by og.order_id
) fr2 on fr2.order_id = oi.order_id
where base.el_type != 'others'
and fr2.order_id is not null
-- , base.el_type
;


count(if(og.sku_order_status = 2 AND ogs.sku_pay_status>0 AND fr.refund_type_id!=2 ,og.order_goods_id,null)) as cancel_cnt;



select
base.pt,
count(distinct base.device_id) AS order_user_uv,
count(distinct base.order_sn) AS order_cnt
from
tmp.tmp_zyzheng_req_base_9415_2 base
-- INNER JOIN dim.dim_vova_order_goods fp on fp.order_sn = base.order_sn
INNER JOIN ods_vova_vts.ods_vova_order_info oi on oi.order_sn = base.order_sn
where base.el_type != 'others'
group by base.pt
order by base.pt

select
case when log.test_info like '%ab_9091_creditcard_fast_payment_b%' then 'ab_9091_creditcard_fast_payment_b'
     when log.test_info like '%ab_9091_creditcard_fast_payment_a%' then 'ab_9091_creditcard_fast_payment_a'
     else log.test_info end as test_info2
log.device_id,
log.extra,
log.pt
from
dwd.dwd_vova_log_goods_impression log
WHERE log.datasource = 'vova'
AND log.platform = 'mob'
AND log.pt = '2021-05-01'
AND log.test_info like '%ab_9091_creditcard_fast_payment%'
group by log.test_info, log.device_id, log.extra, log.pt
;

select
log.pt,
log.test_info,
count(DISTINCT log.device_id) as impression_uv
from
dwd.dwd_vova_log_goods_impression log
WHERE log.datasource = 'vova'
AND log.pt = ''
AND log.platform = 'mob'
group by log.pt
;