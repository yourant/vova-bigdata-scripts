drop table if exists tmp.tmp_zyzheng_8760_tot;
create table tmp.tmp_zyzheng_8760_tot as
select
/*+ REPARTITION(1) */
sum(fp.shop_price * fp.goods_number + fp.shipping_fee) AS gmv,
trunc(date(fp.pay_time), 'MM') AS pay_date
from
dwd.dwd_vova_fact_pay fp
where (fp.pay_time) >= '2020-09-01'
AND (fp.pay_time) < '2021-03-01'
AND fp.datasource = 'vova'
group by trunc(date(fp.pay_time), 'MM')
;

drop table if exists tmp.tmp_zyzheng_8760_tot_1;
create table tmp.tmp_zyzheng_8760_tot_1 as
select
avg(gmv) as gmv,
'2020-09-01' as pay_date
from
tmp.tmp_zyzheng_8760_tot
where pay_date in ('2020-09-01', '2020-10-01')

UNION

select
avg(gmv) as gmv,
'2020-10-01' as pay_date
from
tmp.tmp_zyzheng_8760_tot
where pay_date in ('2020-10-01', '2020-11-01')

UNION

select
avg(gmv) as gmv,
'2020-11-01' as pay_date
from
tmp.tmp_zyzheng_8760_tot
where pay_date in ('2020-11-01', '2020-12-01')

UNION

select
avg(gmv) as gmv,
'2020-12-01' as pay_date
from
tmp.tmp_zyzheng_8760_tot
where pay_date in ('2020-12-01', '2021-01-01')

UNION

select
avg(gmv) as gmv,
'2021-01-01' as pay_date
from
tmp.tmp_zyzheng_8760_tot
where pay_date in ('2021-01-01', '2021-02-01')
;

SELECT
trunc(date(dm.first_publish_time), 'MM') as action_date,
count(distinct dm.mct_id) as mct_cnt,
sum(dog.shop_price * dog.goods_number + dog.shipping_fee) AS gmv,
first(t1.gmv) as tot_gmv,
round(sum(dog.shop_price * dog.goods_number + dog.shipping_fee) / first(t1.gmv), 6) as rate
FROM
dim.dim_vova_merchant dm
inner join dim.dim_vova_order_goods dog on dog.mct_id = dm.mct_id
left join tmp.tmp_zyzheng_8760_tot_1 t1 on t1.pay_date = trunc(date(dm.first_publish_time), 'MM')
where trunc(date(dm.first_publish_time), 'MM') = '2020-08-01'
AND dog.pay_status >= 1
AND dog.sku_order_status != 5
AND dog.datasource = 'vova'
AND date(dog.pay_time) >= '2020-08-01'
AND date(dog.pay_time) < '2021-03-01'
AND datediff(date(dog.pay_time), date(dm.first_publish_time)) >= 0
AND datediff(date(dog.pay_time), date(dm.first_publish_time)) <= 30
group by trunc(date(dm.first_publish_time), 'MM')
;

select count(distinct dm.mct_id),count(*) from dim.dim_vova_merchant dm
;





























