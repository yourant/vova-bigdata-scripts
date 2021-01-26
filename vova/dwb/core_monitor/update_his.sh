#!/bin/bash
#指定日期和引擎
stime=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  stime=`date -d "-1 hour" "+%Y-%m-%d %H:%M:%S"`
fi
echo "$stime"
#默认小时
pt=`date -d "$stime" +%Y-%m-%d`
pre_pt=`date -d "1 day ago ${pt}" +%Y-%m-%d`
pre_week=`date -d "7 day ago ${pt}" +%Y-%m-%d`
echo "$pt"
etime=$2
if [ ! -n "$1" ]; then
  etime=`date -d "0 hour" "+%Y-%m-%d %H:00:00"`
fi
echo "$etime"

sql="
--1、uv相关
drop table if exists tmp.rpt_core_monitor_uv_v2;
create table tmp.rpt_core_monitor_uv_v2 as
select
nvl(hour,25) hour,
nvl(datasource,'all') datasource,
nvl(os_type,'all') os_type,
nvl(app_version,'all') app_version,
nvl(geo_country,'all') geo_country,
count(distinct device_id) dau,
count(distinct pd_device_id) pd_uv,
count(distinct cart_success_device_id) cart_success_uv,
count(distinct checkout_device_id) checkout_uv,
count(distinct payment_device_id)  payment_uv,
count(distinct payment_confirm_device_id) payment_confirm_uv
from
(
select
nvl(t.hour,'NA') hour,
nvl(t.datasource,'NA') datasource,
nvl(t.os_type,'NA') os_type,
nvl(t.app_version,'NA') app_version,
nvl(t.geo_country,'NA') geo_country,
case when t.event_name ='screen_view' THEN t.device_id end device_id,
case when t.page_code ='product_detail' and view_type='show' then t.device_id end pd_device_id,
case when t.element_name ='pdAddToCartSuccess' then t.device_id end cart_success_device_id,
case when t.page_code like '%checkout%' and view_type='show' THEN t.device_id  end checkout_device_id,
case when t.page_code ='payment' and view_type='show' then t.device_id end payment_device_id,
case when t.element_name ='payment_confirm' then t.device_id end payment_confirm_device_id
from
(
select event_name, hour(collector_tstamp) hour,datasource,os_type,device_id,page_code,null element_name ,view_type,app_version,geo_country from dwd.fact_log_screen_view_arc where pt='$pt' and collector_tstamp <'$etime' and platform='mob'
union all
select 'screen_view' event_name, hour(collector_tstamp) hour,datasource,os_type,device_id,page_code,null element_name ,view_type,app_version,geo_country from dwd.fact_log_page_view_arc where pt='$pt' and collector_tstamp <'$etime' and platform='mob' and os_type is not null
union all
select event_name,hour(collector_tstamp) hour,datasource,os_type,device_id,null page_code, element_name ,null view_type,app_version,geo_country from dwd.fact_log_common_click_arc where pt='$pt' and collector_tstamp <'$etime' and platform='mob'
union all
select event_name,hour(collector_tstamp) hour,datasource,os_type,device_id,null page_code, element_name ,null view_type,app_version,geo_country from dwd.fact_log_click_arc where pt='$pt' and collector_tstamp <'$etime' and platform='mob' and event_type='normal'
) t
) t2 group by hour,datasource,os_type,app_version,geo_country with cube;

--获取当前打点的app_version
drop table if exists tmp.rpt_core_monitor_app_version;
create table tmp.rpt_core_monitor_app_version as
select
hour,
datasource,
device_id,
app_version
from
(
select
datasource,
hour,
device_id,
app_version,
row_number() over (partition by device_id,datasource,hour order by collector_tstamp desc) as rank
from
(
select distinct datasource,device_id,app_version,collector_tstamp,hour(collector_tstamp) hour from dwd.fact_log_screen_view_arc where pt='$pt' and collector_tstamp <'$etime' and platform='mob'
union all
select distinct datasource,device_id,app_version,collector_tstamp,hour(collector_tstamp) hour from dwd.fact_log_page_view_arc where pt='$pt' and collector_tstamp <'$etime' and platform='mob' and os_type is not null
) t
) t1 where rank =1;

--gmv相关
drop table if exists tmp.rpt_core_monitor_gmv_v2;
create table tmp.rpt_core_monitor_gmv_v2 as
select
nvl(hour,25) hour,
nvl(datasource,'all') datasource,
nvl(os_type,'all') os_type,
nvl(region_code,'all') region_code,
nvl(app_version,'all') app_version,
sum(goods_amount + shipping_fee) gmv,
count(distinct order_id) payed_order_num,
count(distinct user_id) payed_uv
from
(
select
t.hour,
t.datasource,
t.os_type,
t.region_code,
nvl(ap.app_version,'NA') app_version,
t.order_id,
t.shipping_fee,
t.goods_amount,
t.user_id
from
(
select
hour(pay_time) hour,
case when oi.from_domain like '%vova%' then 'vova'
     when oi.from_domain like '%airyclub%' then 'airyclub'
     end    as datasource,
case when ore.device_type = 11 then 'ios'
     when ore.device_type = 12 then 'android'
     end  as os_type,
oi.order_id,
oi.shipping_fee,
oi.goods_amount,
oi.user_id,
nvl(r.region_code,'NA') region_code,
ore.device_id
from
ods.vova_order_info_h oi
left join ods.vova_order_relation_h ore on ore.order_id = oi.order_id
left join ods.vova_region_h r on r.region_id = oi.country
where oi.pay_status >= 1
and oi.email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
and (oi.from_domain like '%vova%' or oi.from_domain like '%airyclub%')
and ore.device_type in (11,12)
and to_date(pay_time)='$pt' and pay_time <'$etime'
and oi.parent_order_id = 0
) t
left join tmp.rpt_core_monitor_app_version ap on ap.datasource = t.datasource and ap.device_id = t.device_id and ap.hour = t.hour
) t1 group by hour,datasource,os_type,region_code,app_version with cube;

-- ctr相关
drop table if exists tmp.rpt_core_monitor_ctr_v2;
create table tmp.rpt_core_monitor_ctr_v2 as
select
nvl(hour,25) hour,
nvl(datasource,'all') datasource,
nvl(os_type,'all') os_type,
nvl(app_version,'all') app_version,
nvl(geo_country,'all') geo_country,
sum(clicks) clicks,
sum(impressions) impressions,
sum(clicks)/sum(impressions) ctr,
count(distinct click_device_id) click_uv,
count(distinct impression_device_id) impression_uv
from
(
select hour(collector_tstamp) hour,datasource,nvl(os_type,'NA') os_type, device_id click_device_id,null impression_device_id,1 clicks,0 impressions,nvl(app_version,'NA') app_version,nvl(geo_country,'NA') geo_country from dwd.fact_log_goods_click_arc where pt='$pt' and  collector_tstamp <'$etime'  and platform='mob'
union all
select hour(collector_tstamp) hour,datasource,nvl(os_type,'NA') os_type,device_id click_device_id,null impression_device_id,1 clicks,0 impressions,nvl(app_version,'NA') app_version,nvl(geo_country,'NA') geo_country from dwd.fact_log_click_arc where pt='$pt' and collector_tstamp < '$etime' and platform='mob' and event_type='goods'
union all
select hour(collector_tstamp) hour,datasource,nvl(os_type,'NA') os_type,null click_device_id,device_id impression_device_id,0 clicks,1 impressions,nvl(app_version,'NA') app_version,nvl(geo_country,'NA') geo_country from dwd.fact_log_goods_impression_arc where pt='$pt' and  collector_tstamp <'$etime'  and platform='mob'
union all
select hour(collector_tstamp) hour,datasource,nvl(os_type,'NA') os_type,null click_device_id,device_id impression_device_id,0 clicks,1 impressions,nvl(app_version,'NA') app_version,nvl(geo_country,'NA') geo_country from dwd.fact_log_impressions_arc where pt='$pt' and collector_tstamp < '$etime' and platform='mob' and event_type='goods'
) group by hour,datasource,os_type,app_version,geo_country with cube;


drop table if exists tmp.rpt_core_monitor_res_v2;
create table tmp.rpt_core_monitor_res_v2 as
select
/*+ REPARTITION(1) */
u.hour,
u.datasource,
u.os_type,
u.geo_country,
u.app_version,
dau,
pd_uv,
cart_success_uv,
checkout_uv,
payment_uv,
payment_confirm_uv,
nvl(gmv,0) gmv,
nvl(payed_order_num,0) payed_order_num,
nvl(payed_uv,0) payed_uv,
nvl(clicks,0) clicks,
nvl(impressions,0) impressions,
nvl(ctr,0) ctr,
nvl(click_uv,0) click_uv,
nvl(impression_uv,0) impression_uv
from tmp.rpt_core_monitor_uv_v2 u
left join tmp.rpt_core_monitor_gmv_v2 g on u.hour = g.hour and u.datasource = g.datasource and u.os_type =g.os_type and u.app_version = g.app_version and u.geo_country = g.region_code
left join tmp.rpt_core_monitor_ctr_v2 c on u.hour = c.hour and u.datasource = c.datasource and u.os_type =c.os_type and u.app_version = c.app_version and u.geo_country = c.geo_country
where g.os_type!='NA'  and u.os_type!='NA' and c.os_type!='NA';

--获取最新的版本top5
drop table if exists tmp.rpt_core_monitor_app_version_top5;
create table tmp.rpt_core_monitor_app_version_top5 as
select
datasource,
hour,
app_version,
rank
from
(
select
datasource,
hour,
app_version,
row_number() over (partition by datasource,hour order by app_version desc) as rank
from
(
select datasource,app_version,hour
from
(
select datasource,app_version,hour(collector_tstamp) hour from dwd.fact_log_screen_view_arc where pt='$pt' and collector_tstamp <'$etime' and platform='mob'
union all
select datasource,app_version,hour(collector_tstamp) hour from dwd.fact_log_page_view_arc where pt='$pt' and collector_tstamp <'$etime' and platform='mob' and os_type is not null
) t group by datasource,app_version,hour
) t
where app_version not in ('1.9.0','1.8.0','1.7.0','1.6.0','1.5.0','1.3.0','1.2.0','2.7.0','2.8.0','2.9.0')
) t1 where rank<=5;

drop table if exists tmp.rpt_core_monitor_base;
create table tmp.rpt_core_monitor_base as
select
/*+ REPARTITION(1) */
hour,
datasource,
os_type,
region_group,
app_version,
sum(dau) dau,
sum(pd_uv) pd_uv,
sum(cart_success_uv) cart_success_uv,
sum(checkout_uv) checkout_uv,
sum(payment_uv) payment_uv,
sum(payment_confirm_uv) payment_confirm_uv,
sum(gmv) gmv,
sum(payed_order_num) payed_order_num,
sum(payed_uv) payed_uv,
sum(clicks) clicks,
sum(impressions) impressions,
sum(ctr) ctr,
sum(click_uv) click_uv,
sum(impression_uv) impression_uv
from tmp.rpt_core_monitor_res_v2 t
left join tmp.rpt_core_monitor_region_group rg on t.geo_country = rg.region_code
where t.geo_country != 'all' and rg.region_code is not null
group by t.hour,t.datasource,t.os_type,rg.region_group,t.app_version
union all
select
/*+ REPARTITION(1) */
hour,
datasource,
os_type,
geo_country region_group,
app_version,
dau,
pd_uv,
cart_success_uv,
checkout_uv,
payment_uv,
payment_confirm_uv,
gmv,
payed_order_num,
payed_uv,
clicks,
impressions,
ctr,
click_uv,
impression_uv
from tmp.rpt_core_monitor_res_v2 where geo_country = 'all';

insert overwrite table rpt.rpt_core_monitor_base_v2  PARTITION (pt = '$pt')
select
/*+ REPARTITION(1) */
hour,
datasource,
os_type,
region_group,
app_version,
dau,
pd_uv,
cart_success_uv,
checkout_uv,
payment_uv,
payment_confirm_uv,
gmv,
payed_order_num,
payed_uv,
clicks,
impressions,
ctr,
click_uv,
impression_uv
from tmp.rpt_core_monitor_base where app_version = 'all'
union all
select
/*+ REPARTITION(1) */
r.hour,
r.datasource,
r.os_type,
r.region_group,
r.app_version,
r.dau,
r.pd_uv,
r.cart_success_uv,
r.checkout_uv,
r.payment_uv,
r.payment_confirm_uv,
r.gmv,
r.payed_order_num,
r.payed_uv,
r.clicks,
r.impressions,
r.ctr,
r.click_uv,
r.impression_uv
from tmp.rpt_core_monitor_base r
join tmp.rpt_core_monitor_app_version_top5 t on r.datasource = t.datasource and r.hour=t.hour and r.app_version = t.app_version;
"
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" --conf "spark.sql.adaptive.shuffle.targetPostShuffleInputSize=128000000" --conf "spark.sql.adaptive.enabled=true" --conf "spark.app.name=rpt_core_monitor_v2" -e "$sql"
if [ $? -ne 0 ];then
  exit 1
fi