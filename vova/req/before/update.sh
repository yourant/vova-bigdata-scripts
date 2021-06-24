####payment req
select
order_date,
order_count,
user_count,
try_order_count,
try_user_count,
pay_success_order_count,
concat(round(nvl((nvl(try_order_count, 0)) / order_count, 0) * 100 ,2 ),'%') as t1, 订单尝试支付率
concat(round(nvl((nvl(try_user_count, 0)) / user_count, 0) * 100 ,2 ),'%') as t2, 用户尝试支付率
concat(round(nvl((nvl(pay_success_order_count, 0)) / try_order_count, 0) * 100 ,2 ),'%') as t3, 订单尝试支付成功率
concat(round(nvl((nvl(pay_success_order_count, 0)) / order_count, 0) * 100 ,2 ),'%') as t4 订单支付成功率
from
(
select
date(order_time) as order_date,
count(distinct order_sn) AS order_count,
count(distinct user_id) AS user_count,
count(distinct try_order) AS try_order_count,
count(distinct try_user) AS try_user_count,
count(distinct pay_success_order) AS pay_success_order_count
from
(
select
oi.order_time,
oi.order_sn,
oi.user_id,
if(pt.order_sn is not null, oi.order_sn, null) as try_order,
if(pt.order_sn is not null, oi.user_id, null) try_user,
if(oi.pay_status >= 1, oi.order_sn, null) pay_success_order
from
ods_vova_vts.ods_vova_order_info oi
inner join tmp.tmp_zyzheng_0304_2 t2 on t2.buyer_id = oi.user_id and t2.min_order_date = date(oi.order_time)
LEFT JOIN (SELECT order_sn, count(*) AS try_cnt FROM ods_vova_vts.ods_vova_paypal_txn group by order_sn) pt ON pt.order_sn = oi.order_sn
where oi.project_name = 'airyclub'
and oi.country = 4056
and oi.payment_id = 170
AND oi.parent_order_id = 0
AND oi.email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
AND date(oi.order_time) >= '2021-02-01'
AND date(oi.order_time) <= '2021-03-02'
) t1
group by date(order_time)
) t2
order by order_date
;

select
order_date,
order_count,
user_count,
try_order_count,
try_user_count,
pay_success_order_count,
concat(round(nvl((nvl(try_order_count, 0)) / order_count, 0) * 100 ,2 ),'%') as t1,
concat(round(nvl((nvl(try_user_count, 0)) / user_count, 0) * 100 ,2 ),'%') as t2,
concat(round(nvl((nvl(pay_success_order_count, 0)) / try_order_count, 0) * 100 ,2 ),'%') as t3,
concat(round(nvl((nvl(pay_success_order_count, 0)) / order_count, 0) * 100 ,2 ),'%') as t4
from
(
select
date(order_time) as order_date,
count(distinct order_sn) AS order_count,
count(distinct user_id) AS user_count,
count(distinct try_order) AS try_order_count,
count(distinct try_user) AS try_user_count,
count(distinct pay_success_order) AS pay_success_order_count
from
(
select
oi.order_time,
oi.order_sn,
oi.user_id,
if(pt.order_sn is not null, oi.order_sn, null) as try_order,
if(pt.order_sn is not null, oi.user_id, null) try_user,
if(oi.pay_status >= 1, oi.order_sn, null) pay_success_order
from
ods_vova_vts.ods_vova_order_info oi
LEFT JOIN (SELECT order_sn, count(*) AS try_cnt FROM ods_vova_vts.ods_vova_paypal_txn group by order_sn) pt ON pt.order_sn = oi.order_sn
where oi.project_name = 'airyclub'
and oi.country = 4056
and oi.payment_id = 170
AND oi.parent_order_id = 0
AND oi.email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
AND date(oi.order_time) >= '2021-02-01'
AND date(oi.order_time) <= '2021-03-02'
) t1
group by date(order_time)
) t2
order by order_date
;
drop table if exists tmp.tmp_zyzheng_0304_1;
create table tmp.tmp_zyzheng_0304_1 as
select
oi.user_id AS buyer_id,
date(oi.order_time) as order_date
from
ods_vova_vts.ods_vova_order_info oi
where oi.parent_order_id = 0
  and oi.order_time >= '2000-01-01'
  and oi.project_name = 'airyclub'
group by date(oi.order_time),oi.user_id
;

create table tmp.tmp_zyzheng_0304_2 as
select
buyer_id,
min(order_date) as min_order_date
from
tmp.tmp_zyzheng_0304_1 t1
group by buyer_id
;
####payment req end



#### new user req
create table tmp.tmp_zyzheng_new_user_poll_0304_1 as
select
DISTINCT goods_id
FROM
ads.ads_vova_activity_new_user
where biz_type = 'newuser-jingxuan'
;

drop table if exists tmp.tmp_zyzheng_new_user_0304_1;
create table tmp.tmp_zyzheng_new_user_0304_1 as
select
oi.user_id AS buyer_id,
date(oi.order_time) as order_date
from
ods_vova_vts.ods_vova_order_info oi
where oi.parent_order_id = 0
  and oi.order_time >= '2000-01-01'
  and oi.pay_time >= '2000-01-01'
  and oi.project_name = 'vova'
  and oi.pay_status >= 1
group by date(oi.order_time),oi.user_id
;
drop table if exists tmp.tmp_zyzheng_new_user_0304_2;
create table tmp.tmp_zyzheng_new_user_0304_2 as
select
buyer_id,
min(order_date) as min_order_date
from
tmp.tmp_zyzheng_new_user_0304_1 t1
group by buyer_id
;

#首页新人专区曝光人数
create table tmp.tmp_zyzheng_new_user_0304_11 as
select
log.pt,
log.geo_country as region_code,
count(distinct log.device_id) as com_uv
from
dwd.dwd_vova_log_impressions log
where log.pt >= '2021-02-01'
and log.pt <= '2021-02-23'
and log.datasource = 'vova'
and log.platform = 'mob'
and log.geo_country in ('FR', 'DE', 'ES', 'IT')
and log.element_name = 'NewCommer'
group by log.pt, log.geo_country
;

#€3无门槛券领券人数
drop table if exists tmp.tmp_zyzheng_new_user_0304_12;
create table tmp.tmp_zyzheng_new_user_0304_12 as
select
date(dc.cpn_create_time) as pt,
byr.region_code,
count(distinct dc.buyer_id) as tt
FROM dim.dim_vova_coupon dc
  INNER JOIN dim.dim_vova_buyers byr ON byr.buyer_id = dc.buyer_id
WHERE date(dc.cpn_create_time) >= '2021-02-01'
and date(dc.cpn_create_time) <= '2021-02-23'
and dc.cpn_cfg_id = 1726026
and byr.datasource = 'vova'
and byr.region_code in ('FR', 'DE', 'ES', 'IT')
group by date(dc.cpn_create_time), byr.region_code

;
drop table if exists tmp.tmp_zyzheng_new_user_0304_13;
create table tmp.tmp_zyzheng_new_user_0304_13 as
select
date(fp.order_time) as pt,
fp.region_code,
count(distinct fp.buyer_id) as tt
from
dwd.dwd_vova_fact_pay fp
inner join ods_vova_vts.ods_vova_order_info oi on oi.order_id = fp.order_id
inner join tmp.tmp_zyzheng_new_user_0304_2 t2 on t2.buyer_id = fp.buyer_id and date(fp.order_time) = t2.min_order_date
inner join dim.dim_vova_coupon dc on dc.buyer_id = fp.buyer_id
where date(fp.order_time) >= '2021-02-01'
and date(fp.order_time) <= '2021-02-23'
and oi.project_name = 'vova'
and fp.from_domain like '%api%'
and fp.region_code in ('FR', 'DE', 'ES', 'IT')
AND dc.cpn_cfg_id = 1726026
group by date(fp.order_time), fp.region_code

;

#0.02€商品下单人数（1）	新人首单中包含0.02€专区商品人数，该商品购买渠道为整个大盘
drop table if exists tmp.tmp_zyzheng_new_user_0304_14;
create table tmp.tmp_zyzheng_new_user_0304_14 as
select
date(fp.order_time) as pt,
fp.region_code,
count(distinct fp.buyer_id) as tt
from
dwd.dwd_vova_fact_pay fp
inner join ods_vova_vts.ods_vova_order_info oi on oi.order_id = fp.order_id
inner join tmp.tmp_zyzheng_new_user_poll_0304_1 t1 on t1.goods_id = fp.goods_id
inner join tmp.tmp_zyzheng_new_user_0304_2 t2 on t2.buyer_id = fp.buyer_id and date(fp.order_time) = t2.min_order_date
where date(fp.order_time) >= '2021-02-01'
and date(fp.order_time) <= '2021-02-23'
and oi.project_name = 'vova'
and fp.from_domain like '%api%'
and fp.region_code in ('FR', 'DE', 'ES', 'IT')
group by date(fp.order_time), fp.region_code
;



drop table if exists tmp.tmp_zyzheng_new_user_0304_1555;
create table tmp.tmp_zyzheng_new_user_0304_1555 as
select
og.region_code,
og.order_time,
oc.pre_page_code,
oc.pre_element_type,
oc.pre_list_type,
og.buyer_id
from dim.dim_vova_order_goods og
inner join ods_vova_vts.ods_vova_order_info oi on oi.order_id = og.order_id
inner join tmp.tmp_zyzheng_new_user_poll_0304_1 t1 on t1.goods_id = og.goods_id
inner join dwd.dwd_vova_fact_order_cause_v2 oc on og.order_goods_id = oc.order_goods_id
where date(og.order_time) >= '2021-02-01'
and date(og.order_time) <= '2021-02-23'
and og.parent_rec_id =0
and oi.project_name = 'vova'
and og.region_code in ('FR', 'DE', 'ES', 'IT')
and og.from_domain like '%api%'
and og.pay_status >= 1
;

create table tmp.tmp_zyzheng_new_user_0304_15 as
select
date(order_time) as pt,
region_code,
count(distinct buyer_id) as tt
from tmp.tmp_zyzheng_new_user_0304_1555
where pre_page_code = 'theme_activity'
and pre_element_type = 'tejiashangpin'
and pre_list_type = '/tejiashangpin'
group by date(order_time), region_code
;
drop table if exists tmp.tmp_zyzheng_new_user_0304_16;
create table tmp.tmp_zyzheng_new_user_0304_16 as
select
date(order_time) as pt,
region_code,
count(distinct buyer_id) as tt
from tmp.tmp_zyzheng_new_user_0304_1555
where pre_page_code = 'theme_activity'
and pre_element_type in ('xinren', 'xinren01')
and pre_list_type in ('/xinren', '/xinren01')
group by date(order_time), region_code
;

select
t1.pt,
t1.region_code,
t1.com_uv,
t2.tt as tt2,
t3.tt as tt3,
t4.tt as tt4,
t5.tt as tt5,
t6.tt as tt6
from
tmp.tmp_zyzheng_new_user_0304_11 t1
left join tmp.tmp_zyzheng_new_user_0304_12 t2 on t1.pt = t2.pt and t1.region_code = t2.region_code
left join tmp.tmp_zyzheng_new_user_0304_13 t3 on t1.pt = t3.pt and t1.region_code = t3.region_code
left join tmp.tmp_zyzheng_new_user_0304_14 t4 on t1.pt = t4.pt and t1.region_code = t4.region_code
left join tmp.tmp_zyzheng_new_user_0304_15 t5 on t1.pt = t5.pt and t1.region_code = t5.region_code
left join tmp.tmp_zyzheng_new_user_0304_16 t6 on t1.pt = t6.pt and t1.region_code = t6.region_code
order by t1.pt,
t1.region_code

#### new user req end




























































