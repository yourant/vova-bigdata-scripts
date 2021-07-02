create="
drop table tmp.tmp_vova_mct_rel_gmv;
CREATE EXTERNAL TABLE IF NOT EXISTS tmp.tmp_vova_mct_rel_gmv
(
    month     string COMMENT 'd_date',
    first_cat_name string COMMENT 'd_date',
    group_id bigint COMMENT 'd_date',
    goods_id bigint COMMENT 'd_date',
    is_brand bigint COMMENT 'd_date',
    goods_sn string COMMENT 'd_date',
    mct_name string COMMENT 'd_date',
    first_pay_time string COMMENT 'd_date',
    min_re_mct_first_pay_time string COMMENT 'd_date',
    spsor_name string COMMENT 'd_date',
    gmv double COMMENT 'd_date'
) COMMENT 'dwb_vova_web_main_goods'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table tmp.tmp_vova_mct_rel_gmv_d;
CREATE EXTERNAL TABLE IF NOT EXISTS tmp.tmp_vova_mct_rel_gmv_d
(
month string,
first_cat_name string,
group_id string,
goods_id string,
is_brand string,
goods_sn string,
is_self string,
is_test string,
create_time string
) COMMENT 'dwb_vova_web_main_goods'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


 drop table tmp.tmp_dim_vova_mct_rel_gmv_bound;
CREATE EXTERNAL TABLE IF NOT EXISTS tmp.tmp_dim_vova_mct_rel_gmv_bound
(
    first_cat_name string COMMENT 'd_date',
    month     string COMMENT 'd_date',
    gmv_number double COMMENT 'd_date',
    gmv_m double COMMENT 'd_date',
    gmv_d double COMMENT 'd_date'
) COMMENT 'tmp_dim_vova_mct_rel_gmv_bound'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;

"

sql="
with mct_first_pay as (
select
mct_id,
min(pay_time) first_pay_time
from dwd.dwd_vova_fact_pay
where datasource = 'vova'
group by mct_id
),
tmp_group as(
select distinct m.mct_id,t.group_id from mct_first_pay m join ods_vova_vtr.ods_vova_risk_merchant_relation_extra t on m.mct_id = t.merchant_id
),
rela_mct as
(
select
mct_id,
min(first_pay_time) first_pay_time
from
(
select
t.mct_id,
t.re_mct_id,
t.rule_info,
p.first_pay_time
from
(
select distinct g.mct_id,t.merchant_id re_mct_id,t.rule_info from ods_vova_vtr.ods_vova_risk_merchant_relation_extra t join tmp_group g on t.group_id = g.group_id and t.merchant_id != g.mct_id
) t join mct_first_pay p on t.re_mct_id = p.mct_id
) t group by mct_id
)
insert overwrite table tmp.tmp_vova_mct_rel_gmv
select
/*+ REPARTITION(1) */
t.month,
t.first_cat_name,
t.group_id,
t.goods_id,
t.is_brand,
t.goods_sn,
t.mct_name,
t.first_pay_time,
r.first_pay_time min_re_mct_first_pay_time,
t.spsor_name,
t.gmv
from
(
select
month,
first_cat_id,
first_cat_name,
group_id,
goods_id,
is_brand,
goods_sn,
mct_name,
mct_id,
first_pay_time,
spsor_name,
sum(gmv) gmv
from
(
select
trunc(pay_time,'MM') month,
g.first_cat_id,
g.first_cat_name,
gs.group_id,
g.goods_id,
if(g.brand_id>0,1,0) is_brand,
g.goods_sn,
g.mct_name,
g.mct_id,
m.first_pay_time,
m1.spsor_name,
p.shop_price * p.goods_number + p.shipping_fee gmv
from dwd.dwd_vova_fact_pay p
join dim.dim_vova_goods g on p.goods_id = g.goods_id
join dim.dim_vova_merchant m1 on p.mct_id = m1.mct_id
left join mct_first_pay m on p.mct_id = m.mct_id
left join ods_vova_vbts.ods_vova_rec_gid_pic_similar gs on p.goods_id = gs.goods_id
where p.pay_time>='2021-01-01 00:00:00' and p.pay_time<'2021-04-01 00:00:00'
and p.datasource='vova'
) t group by
month,
first_cat_id,
group_id,
first_cat_name,
goods_id,
is_brand,
goods_sn,
mct_name,
mct_id,
first_pay_time,
spsor_name
) t left join rela_mct r on t.mct_id = r.mct_id
;

insert overwrite table tmp.tmp_vova_mct_rel_gmv_d
select
/*+ REPARTITION(1) */
t.month,
t.first_cat_name,
t.group_id,
t.goods_id,
t.is_brand,
t.goods_sn,
t.is_self,
if(t1.goods_id is not null,'Y','N') is_test,
t1.create_time
from
(
select
trunc(pt,'MM') month,
first_cat_name,
group_id,
goods_id,
is_brand,
goods_sn,
is_self
from
(
select
pt,
first_cat_name,
group_id,
goods_id,
is_brand,
goods_sn,
mct_name,
if(mct_name in ('SuperAC','VogueFD'),'Y','N') is_self,
mct_id,
spsor_name,
gmv,
gmv_d,
group_gmv,
big,
res
from
(
select
pt,
first_cat_name,
group_id,
goods_id,
is_brand,
goods_sn,
mct_name,
mct_id,
spsor_name,
gmv,
gmv_d,
group_gmv,
big,
sum(big) over (partition by trunc(pt,'MM'),group_id) res
from
(
select
pt,
first_cat_name,
group_id,
goods_id,
is_brand,
goods_sn,
mct_name,
mct_id,
spsor_name,
gmv,
gmv_d,
group_gmv,
if(group_gmv-nvl(gmv_d,0)>0,1,0) big
from
(
select
t1.pt,
t1.first_cat_name,
t1.group_id,
t1.goods_id,
t1.is_brand,
t1.goods_sn,
t1.mct_name,
t1.mct_id,
t1.spsor_name,
t1.gmv,
t2.gmv_d,
sum(gmv) over(partition by pt,group_id) group_gmv
from
(
select
pt,
first_cat_name,
group_id,
goods_id,
is_brand,
goods_sn,
mct_name,
mct_id,
spsor_name,
sum(gmv) gmv
from
(
select
to_date(pay_time) pt,
g.first_cat_name,
if(gs.group_id is null,concat('G',g.goods_id),gs.group_id) group_id,
g.goods_id,
if(g.brand_id>0,1,0) is_brand,
g.goods_sn,
g.mct_name,
g.mct_id,
m1.spsor_name,
p.shop_price * p.goods_number + p.shipping_fee gmv
from dwd.dwd_vova_fact_pay p
join dim.dim_vova_goods g on p.goods_id = g.goods_id
join dim.dim_vova_merchant m1 on p.mct_id = m1.mct_id
left join ods_vova_vbts.ods_vova_rec_gid_pic_similar gs on p.goods_id = gs.goods_id
where p.pay_time>='2021-01-01 00:00:00' and p.pay_time<'2021-04-01 00:00:00'
and p.datasource='vova'
) t
group by  pt,
first_cat_name,
group_id,
goods_id,
is_brand,
goods_sn,
mct_name,
mct_id,
spsor_name
) t1 left join tmp.tmp_dim_vova_mct_rel_gmv_bound t2 on trunc(t1.pt,'MM') =t2.month and t1.first_cat_name=t2.first_cat_name
) t
) t
) t where res>=7
) t group by
trunc(pt,'MM'),
first_cat_name,
group_id,
goods_id,
is_brand,
goods_sn,
is_self
) t left join
(
select
goods_id,
create_time
from
(
select
goods_id,
create_time,
row_number() over(partition by goods_id order by create_time) rank
from ods_vova_vbd.ods_vova_test_goods_behave
) t where rank =1
) t1 on t.goods_id = t1.goods_id;




select
month,
goods_id,
first_cat_name,
group_id,
goods_sn,
mct_name,
first_pay_time,
min_re_mct_first_pay_time,
group_gmv_m,
gmv_m
from
(
select
month,
first_cat_name,
group_id,
goods_id,
mct_name,
first_pay_time,
min_re_mct_first_pay_time,
goods_sn,
sum(gmv) over(partition by group_id,month) group_gmv_m,
gmv_m
from
(
select
month,
first_cat_name,
if(group_id is not null,group_id,concat('A',group_number)) group_id,
goods_id,
mct_name,
goods_sn,
first_pay_time,
min_re_mct_first_pay_time,
gmv_m,
gmv
from
(
select
t1.month,
t1.first_cat_name,
t1.group_id,
t1.goods_id,
t1.mct_name,
t1.first_pay_time,
t1.min_re_mct_first_pay_time,
goods_sn,
row_number() over(partition by year(t1.month) order by group_id) group_number,
t2.gmv_m,
t1.gmv
from tmp.tmp_vova_mct_rel_gmv t1
left join tmp.tmp_dim_vova_mct_rel_gmv_bound t2 on t1.month =t2.month and t1.first_cat_name=t2.first_cat_name
where to_date(t1.first_pay_time)>='2021-01-01' and to_date(t1.first_pay_time)<'2021-04-01'
and to_date(t1.min_re_mct_first_pay_time)>='2021-01-01' and to_date(t1.min_re_mct_first_pay_time)<'2021-04-01'
) t
) t
) t where group_gmv_m>gmv_m

"




