drop table if exists tmp.tmp_zyzheng_req_base2_0610;
create table tmp.tmp_zyzheng_req_base2_0610 as
select
log.device_id,
log.dvce_created_ts,
log.pt,
log.page_code,
null as order_id
from
dwd.dwd_vova_log_screen_view log
where log.pt >= '2021-05-19'
and log.pt <= '2021-06-10'
and log.platform = 'mob'
and log.datasource = 'vova'
and log.page_code in ('checkout_new')

union all

SELECT
first(og.device_id) AS device_id,
first(og.order_time) AS order_time,
first(date(og.order_time)) AS order_date,
'order' AS page_code,
og.order_id
from
dim.dim_vova_order_goods og
  left join dim.dim_vova_goods g on g.goods_id = og.goods_id
  where date(og.order_time) >= '2021-05-19'
  and date(og.order_time) <= '2021-06-10'
  AND og.datasource = 'vova'
  AND og.parent_order_id = 0
  and og.platform in('ios','android')
group by og.order_id
;

drop table if exists tmp.tmp_zyzheng_req_base3_0610;
create table tmp.tmp_zyzheng_req_base3_0610 as
select
t1.device_id,
t1.order_id,
t1.pt
from
(
SELECT
log.pt,
log.device_id,
log.page_code,
log.order_id,
log.dvce_created_ts,
lag(log.page_code,1) over(partition by log.device_id,log.pt order by log.dvce_created_ts) lag_page_code,
lag(log.dvce_created_ts,1) over(partition by log.device_id,log.pt order by log.dvce_created_ts) lag_dvce_created_ts
from
tmp.tmp_zyzheng_req_base2_0610 log
where log.device_id is not null
) t1
where t1.page_code = 'order'
and t1.lag_page_code = 'checkout_new'
and unix_timestamp(t1.dvce_created_ts) - unix_timestamp(t1.lag_dvce_created_ts) <= 180
;
select count(*),count(distinct order_id) from tmp.tmp_zyzheng_req_base3_0610;








drop table if exists tmp.tmp_zyzheng_req_base1_0610;
create table tmp.tmp_zyzheng_req_base1_0610 as
select
log.device_id,
log.dvce_created_ts,
log.page_code,
log.pt
from
dwd.dwd_vova_log_screen_view log
where log.pt >= '2021-05-19'
and log.pt <= '2021-06-10'
and log.platform = 'mob'
and log.datasource = 'vova'
and log.page_code in ('product_detail', 'cart', 'checkout_new')
UNION ALL

select
log.device_id,
log.dvce_created_ts,
'product_detail_buy_now' AS page_code,
log.pt
from dwd.dwd_vova_log_click_arc log
where log.pt>='2021-05-19'
and log.pt <= '2021-06-10'
and log.platform = 'mob'
and log.datasource = 'vova'
and log.page_code = 'product_detail'
and log.event_type ='normal'
and log.event_name = 'click'
and element_name in ('buy_now_at_product_detail','buy_now_at_product_options_dialog')
;








unix_timestamp(t1.dvce_created_ts) - unix_timestamp(t1.lag_dvce_created_ts) as t_diff
,

drop table if exists tmp.tmp_zyzheng_req_base4_0610;
create table tmp.tmp_zyzheng_req_base4_0610 as
select
t1.device_id,
t1.pt
from
(
SELECT
log.pt,
log.device_id,
log.page_code,
log.dvce_created_ts,
lag(log.page_code,1) over(partition by log.device_id,log.pt order by log.dvce_created_ts) lag_page_code,
lag(log.dvce_created_ts,1) over(partition by log.device_id,log.pt order by log.dvce_created_ts) lag_dvce_created_ts
from
tmp.tmp_zyzheng_req_base1_0610 log
where log.device_id is not null
AND log.page_code in ('checkout_new', 'product_detail_buy_now')
) t1
where t1.page_code = 'checkout_new'
and t1.lag_page_code = 'product_detail_buy_now'
and unix_timestamp(t1.dvce_created_ts) - unix_timestamp(t1.lag_dvce_created_ts) <= 120
group by t1.device_id,
t1.pt
;


drop table if exists tmp.tmp_zyzheng_req_base5_0610;
create table tmp.tmp_zyzheng_req_base5_0610 as
select
t1.device_id,
t1.pt
from
(
SELECT
log.pt,
log.device_id,
log.page_code,
log.dvce_created_ts,
lag(log.page_code,1) over(partition by log.device_id,log.pt order by log.dvce_created_ts) lag_page_code,
lag(log.dvce_created_ts,1) over(partition by log.device_id,log.pt order by log.dvce_created_ts) lag_dvce_created_ts
from
tmp.tmp_zyzheng_req_base1_0610 log
where log.device_id is not null
) t1
where t1.page_code = 'checkout_new'
and t1.lag_page_code = 'cart'
and unix_timestamp(t1.dvce_created_ts) - unix_timestamp(t1.lag_dvce_created_ts) <= 300
group by t1.device_id,
t1.pt
;


--------
drop table if exists tmp.tmp_zyzheng_req_base6_0610;
create table tmp.tmp_zyzheng_req_base6_0610 as
select
t1.device_id,
t1.pt
from
(
SELECT
log.pt,
log.device_id,
log.page_code,
log.dvce_created_ts,
lag(log.page_code,1) over(partition by log.device_id,log.pt order by log.dvce_created_ts) lag_page_code,
lag(log.dvce_created_ts,1) over(partition by log.device_id,log.pt order by log.dvce_created_ts) lag_dvce_created_ts
from
tmp.tmp_zyzheng_req_base1_0610 log
where log.device_id is not null
) t1
where t1.page_code = 'checkout_new'
and t1.lag_page_code = 'product_detail'
and unix_timestamp(t1.dvce_created_ts) - unix_timestamp(t1.lag_dvce_created_ts) <= 120
;
------



drop table if exists tmp.tmp_zyzheng_req_base_0610_fin;
create table tmp.tmp_zyzheng_req_base_0610_fin as
select
t1.device_id,
t1.order_id,
t1.pt,
case
when t4.device_id is not null then 'A'
when t5.device_id is not null then 'B'
ELSE 'C' END as el_type
from
tmp.tmp_zyzheng_req_base3_0610 t1
left join tmp.tmp_zyzheng_req_base4_0610 t4 on t4.device_id = t1.device_id and t4.pt = t1.pt
left join tmp.tmp_zyzheng_req_base5_0610 t5 on t5.device_id = t1.device_id and t5.pt = t1.pt
;



drop table if exists tmp.tmp_zyzheng_req_base_0610_ab;
create table tmp.tmp_zyzheng_req_base_0610_ab as
select
first(
case when log.test_info like '%ab_9332_pay_now_a%' then 'ab_9332_pay_now_a'
when log.test_info like '%ab_9332_pay_now_b%' then 'ab_9332_pay_now_b'
else '' end
) as tt_info,
log.device_id,
log.pt
from
dwd.dwd_vova_log_data log
WHERE log.datasource = 'vova'
AND log.platform = 'mob'
AND log.pt >= '2021-05-19'
AND log.pt <= '2021-06-10'
AND log.test_info like '%ab_9332_pay_now%'
group by
log.device_id,
log.pt
;

select pt,el_type,count(*),count(distinct order_id),count(distinct device_id) from tmp.tmp_zyzheng_req_base_0610_fin group by pt,el_type;
select count(*),count(distinct order_id),count(distinct device_id) from tmp.tmp_zyzheng_req_base_0610_fin where device_id is not null;
select * from tmp.tmp_zyzheng_req_base_0610_fin where device_id = '';

select
fin.pt,
count(distinct if(fin.el_type in ('A', 'C'), fin.order_id, null)) AS buy_cnt,
sum(if(fin.el_type in ('A', 'C'), oi.goods_amount + oi.shipping_fee, 0)) AS buy_gmv,
count(distinct if(fin.el_type in ('A', 'C') AND oi.pay_status >= 1, fin.order_id, null)) AS buy_cnt2,
sum(if(fin.el_type in ('A', 'C') AND oi.pay_status >= 1, oi.goods_amount + oi.shipping_fee, 0)) AS buy_gmv2,
count(distinct if(fin.el_type in ('B'), fin.order_id, null)) AS buy2_cnt,
sum(if(fin.el_type in ('B'), oi.goods_amount + oi.shipping_fee, 0)) AS buy2_gmv,
count(distinct if(fin.el_type in ('B') AND oi.pay_status >= 1, fin.order_id, null)) AS buy2_cnt2,
sum(if(fin.el_type in ('B') AND oi.pay_status >= 1, oi.goods_amount + oi.shipping_fee, 0)) AS buy2_gmv2
from
tmp.tmp_zyzheng_req_base_0610_fin fin
inner join ods_vova_vts.ods_vova_order_info oi on oi.order_id = fin.order_id
where email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
group by fin.pt
;

select count(*),count(distinct device_id) from tmp.tmp_zyzheng_req_base_0610_ab;
select distinct tt_info from tmp.tmp_zyzheng_req_base_0610_ab;

select
fin.pt,
count(distinct if(ab.tt_info = 'ab_9332_pay_now_a', fin.order_id, null)) AS buy_cnt,
sum(if(ab.tt_info = 'ab_9332_pay_now_a', oi.goods_amount + oi.shipping_fee, 0)) AS buy_gmv,
count(distinct if(ab.tt_info = 'ab_9332_pay_now_a' AND oi.pay_status >= 1, fin.order_id, null)) AS buy_cnt2,
sum(if(ab.tt_info = 'ab_9332_pay_now_a' AND oi.pay_status >= 1, oi.goods_amount + oi.shipping_fee, 0)) AS buy_gmv2,
count(distinct if(ab.tt_info = 'ab_9332_pay_now_b', fin.order_id, null)) AS buy_cnt,
sum(if(ab.tt_info = 'ab_9332_pay_now_b', oi.goods_amount + oi.shipping_fee, 0)) AS buy_gmv,
count(distinct if(ab.tt_info = 'ab_9332_pay_now_b' AND oi.pay_status >= 1, fin.order_id, null)) AS buy_cnt2,
sum(if(ab.tt_info = 'ab_9332_pay_now_b' AND oi.pay_status >= 1, oi.goods_amount + oi.shipping_fee, 0)) AS buy_gmv2
from
tmp.tmp_zyzheng_req_base_0610_fin fin
inner join tmp.tmp_zyzheng_req_base_0610_ab ab on ab.device_id = fin.device_id and ab.pt = fin.pt
inner join ods_vova_vts.ods_vova_order_info oi on oi.order_id = fin.order_id
where fin.el_type in ('A', 'C')
and oi.email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
group by fin.pt
;

