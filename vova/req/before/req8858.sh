###copy #8737
###n=2,3,4,5,10
drop table if exists tmp.tmp_zyzheng_8858_base;
create table tmp.tmp_zyzheng_8858_base as
select
/*+ REPARTITION(1) */
order_time,
order_goods_id,
buyer_id,
is_refund,
sku_pay_status,
rank1
from
(
select
order_time,
order_goods_id,
buyer_id,
is_refund,
sku_pay_status,
row_number() over (partition by buyer_id order by order_time asc,is_refund desc ) as rank1
from
(
select
dog.order_time,
dog.order_goods_id,
dog.buyer_id,
if(fr.order_goods_id is not null and fr.refund_type_id = 2 and fr.refund_reason_type_id != 8, 1, 0) as is_refund,
dog.sku_pay_status
from
dim.dim_vova_order_goods dog
left join dwd.dwd_vova_fact_refund fr on fr.order_goods_id = dog.order_goods_id
where (dog.order_time) >= '2020-09-01'
and (dog.order_time) <= '2020-11-30'
and dog.datasource = 'vova'
and dog.pay_status >= 1
and dog.sku_order_status != 5
and dog.sku_shipping_status = 2
and dog.email NOT REGEXP '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
) t1
) t2
;

drop table if exists tmp.tmp_zyzheng_8858_base_tot;
create table tmp.tmp_zyzheng_8858_base_tot as
select
/*+ REPARTITION(1) */
'a' as typea,
sum(dog.shop_price * dog.goods_number + dog.shipping_fee) as gmv,
count(distinct dog.buyer_id) as tot_paid_buyer_cnt,
count(distinct dog.order_goods_id) as tot_confirm_order_cnt,
sum(if(fr.order_goods_id is not null, fr.refund_amount, 0)) as tot_refund_amount,
count(distinct if(fr.order_goods_id is not null, fr.order_goods_id, null)) as tot_refund_order_cnt,
sum(if(dog.sku_pay_status = 4, fr.refund_amount, 0)) as tot_refund_amount2,
count(distinct if(dog.sku_pay_status = 4, fr.order_goods_id, null)) as tot_refund_order_cnt2
from
dim.dim_vova_order_goods dog
left join dwd.dwd_vova_fact_refund fr on fr.order_goods_id = dog.order_goods_id
where (dog.order_time) >= '2020-09-01'
and (dog.order_time) <= '2020-11-30'
and (dog.confirm_time) >= '2020-09-01'
and dog.datasource = 'vova'
and dog.pay_status >= 1
and dog.sku_order_status != 5
and dog.email NOT REGEXP '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
;



drop table if exists tmp.tmp_zyzheng_8858_base_bl_3;
create table tmp.tmp_zyzheng_8858_base_bl_3 as
select
/*+ REPARTITION(1) */
'a' as typea,
sum(dog.shop_price * dog.goods_number + dog.shipping_fee) as bl_gmv,
count(distinct dog.buyer_id) as bl_paid_buyer_cnt,
count(distinct dog.order_goods_id) as bl_confirm_order_cnt,
sum(if(fr.order_goods_id is not null, fr.refund_amount, 0))  as bl_refund_amount,
count(distinct if(fr.order_goods_id is not null, fr.order_goods_id, null)) as bl_refund_order_cnt,
sum(if(dog.sku_pay_status = 4, fr.refund_amount, 0)) as bl_refund_amount2,
count(distinct if(dog.sku_pay_status = 4, fr.order_goods_id, null)) as bl_refund_order_cnt2
from
dim.dim_vova_order_goods dog
inner join
 (
select
distinct buyer_id
from
(
select
buyer_id,
count(*) as a,
sum(is_refund) as b,
sum(if(sku_pay_status = 4, 1, 0)) as c
from
tmp.tmp_zyzheng_8858_base
where rank1 <= 3
group by buyer_id
having a = 3 and b = 3
)ttt
 ) t1 on t1.buyer_id = dog.buyer_id
left join dwd.dwd_vova_fact_refund fr on fr.order_goods_id = dog.order_goods_id
where (dog.order_time) >= '2020-09-01'
and (dog.order_time) <= '2020-11-30'
and (dog.confirm_time) >= '2020-09-01'
and dog.datasource = 'vova'
and dog.pay_status >= 1
and dog.sku_order_status != 5
and dog.email NOT REGEXP '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
;

drop table if exists tmp.tmp_zyzheng_8858_base_bl_33;
create table tmp.tmp_zyzheng_8858_base_bl_33 as
select
/*+ REPARTITION(1) */
'a' as typea,
sum(dog.shop_price * dog.goods_number + dog.shipping_fee) as bl_gmv,
count(distinct dog.buyer_id) as bl_paid_buyer_cnt,
count(distinct dog.order_goods_id) as bl_confirm_order_cnt,
sum(if(fr.order_goods_id is not null, fr.refund_amount, 0))  as bl_refund_amount,
count(distinct if(fr.order_goods_id is not null, fr.order_goods_id, null)) as bl_refund_order_cnt,
sum(if(dog.sku_pay_status = 4, fr.refund_amount, 0)) as bl_refund_amount2,
count(distinct if(dog.sku_pay_status = 4, fr.order_goods_id, null)) as bl_refund_order_cnt2
from
dim.dim_vova_order_goods dog
inner join
 (
select
distinct buyer_id
from
(
select
buyer_id,
count(*) as a,
sum(is_refund) as b,
sum(if(sku_pay_status = 4, 1, 0)) as c
from
tmp.tmp_zyzheng_8858_base
where rank1 <= 3
group by buyer_id
having a = 3 and b = 3 and c = 3
)ttt
 ) t1 on t1.buyer_id = dog.buyer_id
left join dwd.dwd_vova_fact_refund fr on fr.order_goods_id = dog.order_goods_id
where (dog.order_time) >= '2020-09-01'
and (dog.order_time) <= '2020-11-30'
and (dog.confirm_time) >= '2020-09-01'
and dog.datasource = 'vova'
and dog.pay_status >= 1
and dog.sku_order_status != 5
and dog.email NOT REGEXP '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
;


select
t2.bl_paid_buyer_cnt,
tot.tot_paid_buyer_cnt,
round(t2.bl_gmv / tot.gmv, 4) as a1,
round(t2.bl_refund_order_cnt2 / t2.bl_confirm_order_cnt, 4) as a21,
round(tot.tot_refund_order_cnt2 / tot.tot_confirm_order_cnt, 4) as a22,
round(t2.bl_refund_amount2 / t2.bl_gmv, 4) as a23,
round(tot.tot_refund_amount2 / tot.gmv, 4) as a24,
round(t2.bl_refund_order_cnt / t2.bl_confirm_order_cnt, 4) as a31,
round(tot.tot_refund_order_cnt / tot.tot_confirm_order_cnt, 4) as a32,
round(t2.bl_refund_amount / t2.bl_gmv, 4) as a33,
round(tot.tot_refund_amount / tot.gmv, 4) as a34
from
tmp.tmp_zyzheng_8858_base_bl_33 t2
inner join tmp.tmp_zyzheng_8858_base_tot tot on tot.typea = t2.typea
;



select
buyer_id,

from
(
select
buyer_id,
count(*) as a,
sum(is_refund) as b,
sum(if(sku_pay_status = 4, 1, 0)) as c
from
tmp.tmp_zyzheng_8858_base
where rank1 <= 3
group by buyer_id
having a = 3 and b = 3 and c = 3
) t1
inner join tmp.tmp_zyzheng_8858_base t2 on t1.buyer_id = t2.buyer_id

;


select
distinct dog.email
from
(
select
buyer_id,
count(*) as a,
sum(is_refund) as b,
sum(if(sku_pay_status = 4, 1, 0)) as c
from
tmp.tmp_zyzheng_8858_base
where rank1 <= 3
group by buyer_id
having a = 3 and b = 3 and c = 3
)ttt
inner join dim.dim_vova_order_goods  dog on dog.buyer_id = ttt.buyer_id
where (dog.order_time) >= '2020-09-01'
and (dog.order_time) <= '2020-11-30'
and (dog.confirm_time) >= '2020-09-01'
and dog.datasource = 'vova'
and dog.pay_status >= 1
and dog.sku_order_status != 5
and dog.email NOT REGEXP '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'