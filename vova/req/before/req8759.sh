#
sh /mnt/vova-bigdata-scripts/common/sqoop_import_themis_2.sh --db_code=vts --table_name=shopping_cart --etl_type=ALL  --mapers=10 --period_type=day --partition_num=10
sh /mnt/vova-bigdata-scripts/common/sqoop_import_themis_2.sh --db_code=vts --table_name=users_goods_favorites --etl_type=ALL  --mapers=10 --period_type=day --partition_num=10

select user_id,count(DISTINCT goods_id) as goods_cnt from users_goods_favorites where is_delete = 0 group by user_id;


select count(*),count(distinct buyer_id),geo_country from tmp.tmp_zyzheng_8759_base group by geo_country;
select count(*),count(distinct buyer_id),geo_country from tmp.tmp_zyzheng_8759_0311_1 group by geo_country;
select count(*),count(distinct email),geo_country from tmp.tmp_zyzheng_8759_0311_2 group by geo_country;
select count(*),count(distinct buyer_id),geo_country from tmp.tmp_zyzheng_8759_1 group by geo_country;
select count(*),count(distinct email),geo_country from tmp.tmp_zyzheng_8759_2 group by geo_country;
select count(*),count(distinct email),region_code from tmp.tmp_zyzheng_8759_base22 group by region_code;
select count(*),count(distinct email),region_code from tmp.tmp_zyzheng_8759_base_device444 group by region_code;



drop table if exists tmp.tmp_zyzheng_8759_buyer;
create table tmp.tmp_zyzheng_8759_buyer as
select
db.buyer_id
from
dim.dim_vova_buyers db
left join (select distinct user_id  from ods_vova_vts.ods_vova_order_info where parent_order_id = 0 and pay_status >= 1 and project_name = 'vova') t2 on t2.user_id = db.buyer_id
where db.datasource = 'vova'
AND t2.user_id is null
AND date(db.bind_time) >= '2020-01-01'
;


drop table if exists tmp.tmp_zyzheng_8759_base_device;
create table tmp.tmp_zyzheng_8759_base_device as
select
/*+ REPARTITION(5) */
log.device_id,
max(log.pt) as max_pt,
min(log.pt) as min_pt,
last(log.geo_country) AS geo_country,
last(log.language) AS language
from
dwd.dwd_vova_log_screen_view log
where log.datasource = 'vova'
and log.platform = 'mob'
and log.pt>= '2020-12-01'
and log.pt<= '2021-03-10'
AND log.geo_country IN ('GB', 'FR', 'DE', 'IT', 'ES')
group by log.device_id
;


drop table if exists tmp.tmp_zyzheng_8759_0311_1;
create table tmp.tmp_zyzheng_8759_0311_1 as
select
buyer_id,
max_pt,
min_pt,
geo_country,
language,
rank
from
(
select
db.buyer_id,
t1.max_pt,
t1.min_pt,
t1.geo_country,
t1.language,
row_number() OVER (PARTITION BY t1.geo_country ORDER BY t1.max_pt asc) AS rank
from
tmp.tmp_zyzheng_8759_base_device t1
inner join dim.dim_vova_buyers db on db.current_device_id = t1.device_id
inner join tmp.tmp_zyzheng_8759_buyer t2 on t2.buyer_id = db.buyer_id
left join tmp.tmp_zyzheng_8759_2 t3 on t3.email = db.email
where t1.geo_country IN ('GB', 'FR', 'DE', 'IT', 'ES')
and db.email is not null
and t3.email is null
) t1
where rank< 100000
;

select
count(*)
from
tmp.tmp_zyzheng_8759_0311_2 t1
inner join tmp.tmp_zyzheng_8759_2 t2 on t1.email=t2.email
;
drop table if exists tmp.tmp_zyzheng_8759_2;
create table tmp.tmp_zyzheng_8759_2 as;
drop table if exists tmp.tmp_zyzheng_8759_0311_2;
create table tmp.tmp_zyzheng_8759_0311_2 as
select
email,
bind_time,
max_pt,
is_favorites,
is_cart,
language,
geo_country
from
(
select
email,
buyer_id,
bind_time,
max_pt,
is_favorites,
is_cart,
language,
geo_country,
row_number() OVER (PARTITION BY geo_country order by buyer_id desc) AS rank
from
(
select
email,
first(buyer_id) as buyer_id,
first(bind_time) as bind_time,
first(max_pt) as max_pt,
first(is_favorites) as is_favorites,
first(is_cart) as is_cart,
first(language) as language,
first(geo_country) as geo_country
from
(
select
db.email,
db.buyer_id,
db.bind_time,
t1.max_pt,
if(t2.user_id is not null, 'Y', 'N') AS is_favorites,
if(t3.user_id is not null, 'Y', 'N') AS is_cart,
t1.language,
t1.geo_country
from
tmp.tmp_zyzheng_8759_0311_1 t1
inner join dim.dim_vova_buyers db on db.buyer_id = t1.buyer_id
left join (SELECT distinct user_id FROM ods_vova_vts.ods_vova_users_goods_favorites t2 where t2.is_delete = 0 ) t2 on t2.user_id = t1.buyer_id
left join (SELECT distinct user_id FROM ods_vova_vts.ods_vova_shopping_cart t3 where user_id > 0 ) t3 on t3.user_id = t1.buyer_id
left join tmp.tmp_zyzheng_8759_2 t3 on t3.email = db.email
and t3.email is null
and db.email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
) t1
group by email
) t2
) t3
where  t3.rank <=10000
;

select
email,
bind_time,
max_pt,
is_cart,
is_favorites,
language,
geo_country
from tmp.tmp_zyzheng_8759_0311_2
;


#step2
drop table if exists tmp.tmp_zyzheng_8759_base22;
create table tmp.tmp_zyzheng_8759_base22 as
select
t1.email,
t1.buyer_id,
t1.min_pay_date,
t1.max_pay_date,
t1.language,
t1.region_code,
t2.order_cnt,
db.current_device_id
from
(
select
fp.buyer_email as email,
min(date(fp.pay_time)) as min_pay_date,
max(date(fp.pay_time)) as max_pay_date,
first(fp.buyer_id) as buyer_id,
first(dl.languages_code) as language,
first(fp.region_code) as region_code
from
dwd.dwd_vova_fact_pay fp
inner join ods_vova_vts.ods_vova_order_info oi on oi.order_id = fp.order_id
inner join dim.dim_vova_languages dl on dl.languages_id = oi.language_id
where date(fp.pay_time) >= '2020-11-09'
and date(fp.pay_time) < '2021-03-09'
and fp.datasource = 'vova'
and fp.region_code in ('GB', 'FR', 'DE', 'IT', 'ES')
and fp.buyer_email is not null
group by fp.buyer_email
) t1
left join
(
select
fp.buyer_email as email,
count(distinct order_id) as order_cnt
from
dwd.dwd_vova_fact_pay fp
where date(pay_time) >= '2018-01-01'
and fp.buyer_email is not null
group by fp.buyer_email
) t2 on t2.email = t1.email
inner join dim.dim_vova_buyers db on db.buyer_id = t1.buyer_id
where db.current_device_id is not null
;

drop table if exists tmp.tmp_zyzheng_8759_base_device2;
create table tmp.tmp_zyzheng_8759_base_device2 as
select
/*+ REPARTITION(5) */
log.device_id,
trunc(log.pt, 'MM') as start_month
from
dwd.dwd_vova_log_screen_view log
inner join tmp.tmp_zyzheng_8759_base22 t1 on t1.current_device_id = log.device_id
where log.datasource = 'vova'
and log.platform = 'mob'
and log.pt>= '2020-11-09'
and log.pt< '2021-03-09'
group by log.device_id,trunc(log.pt, 'MM')
;

drop table if exists tmp.tmp_zyzheng_8759_base_device234;
create table tmp.tmp_zyzheng_8759_base_device234 as
select
/*+ REPARTITION(5) */
log.device_id,
max(log.pt) as max_pt
from
dwd.dwd_vova_log_screen_view log
inner join tmp.tmp_zyzheng_8759_base22 t1 on t1.current_device_id = log.device_id
where log.datasource = 'vova'
and log.platform = 'mob'
and log.pt>= '2020-11-09'
and log.pt< '2021-03-09'
group by log.device_id
;


drop table if exists tmp.tmp_zyzheng_8759_base_device222;
create table tmp.tmp_zyzheng_8759_base_device222 as
select
device_id,
ceil(months_between(max_start_month, min_pay_date_month)) as diff_month
from
(
select
log.device_id,
max(start_month) as max_start_month,
min(trunc(t1.min_pay_date, 'MM')) as min_pay_date_month
from
tmp.tmp_zyzheng_8759_base_device2 log
inner join tmp.tmp_zyzheng_8759_base22 t1 on t1.current_device_id = log.device_id
group by log.device_id
) t1

;



drop table if exists tmp.tmp_zyzheng_8759_base_device444;
create table tmp.tmp_zyzheng_8759_base_device444 as
select
t1.email,
if(floor(months_between(t3.max_pt, t1.min_pay_date)) <=1,if(floor(months_between(t3.max_pt, t1.min_pay_date)) <=0,1,2), 3) as diff2,
t1.order_cnt,
t1.max_pay_date,
t3.max_pt,
t1.language,
t1.region_code,
t1.current_device_id,
t1.buyer_id,
if(t2.diff_month <=1,if(t2.diff_month <=0,1,2), 3) as diff,
t1.min_pay_date
from
(
select
t1.email,
t1.buyer_id,
t1.min_pay_date,
t1.max_pay_date,
t1.language,
t1.region_code,
t1.order_cnt,
t1.current_device_id,
row_number() OVER (PARTITION BY t1.region_code order by t1.min_pay_date asc) AS rank
from
tmp.tmp_zyzheng_8759_base22 t1
) t1
left join tmp.tmp_zyzheng_8759_base_device222 t2 on t2.device_id = t1.current_device_id
left join tmp.tmp_zyzheng_8759_base_device234 t3 on t3.device_id = t1.current_device_id
where t1.rank <=5000
;
