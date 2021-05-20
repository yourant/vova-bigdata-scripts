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

hour=$(( ${stime:11:1} * 10 + ${stime:12:1}))
echo "$hour"
echo "$etime"

sql="
--1、uv相关
with rpt_core_monitor_uv as (
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
select event_name, hour(collector_ts) hour,datasource,os_type,device_id,page_code,null element_name ,view_type,app_version,geo_country from dwd.dwd_vova_log_screen_view_arc where pt='$pt' and collector_ts <'$etime' and platform='mob'
union all
select 'screen_view' event_name, hour(collector_ts) hour,datasource,os_type,device_id,page_code,null element_name ,view_type,app_version,geo_country from dwd.dwd_vova_log_page_view_arc where pt='$pt' and collector_ts <'$etime' and platform='mob' and os_type is not null
union all
select event_name,hour(collector_ts) hour,datasource,os_type,device_id,null page_code, element_name ,null view_type,app_version,geo_country from dwd.dwd_vova_log_common_click_arc where pt='$pt' and collector_ts <'$etime' and platform='mob'
union all
select event_name,hour(collector_ts) hour,datasource,os_type,device_id,null page_code, element_name ,null view_type,app_version,geo_country from dwd.dwd_vova_log_click_arc where pt='$pt' and collector_ts <'$etime' and platform='mob' and event_type='normal'
union all
select event_name,hour(collector_ts) hour,datasource,os_type,device_id,null page_code, element_name ,null view_type,app_version,geo_country from dwd.dwd_vova_log_data_arc where pt='$pt' and collector_ts <'$etime' and platform='mob' and element_name ='pdAddToCartSuccess'
) t
) t2 group by hour,datasource,os_type,app_version,geo_country with cube
),
--获取当前打点的app_version
rpt_core_monitor_app_version as (
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
row_number() over (partition by device_id,datasource,hour order by collector_ts desc) as rank
from
(
select distinct datasource,device_id,app_version,collector_ts,hour(collector_ts) hour from dwd.dwd_vova_log_screen_view_arc where pt='$pt' and collector_ts <'$etime' and platform='mob'
union all
select distinct datasource,device_id,app_version,collector_ts,hour(collector_ts) hour from dwd.dwd_vova_log_page_view_arc where pt='$pt' and collector_ts <'$etime' and platform='mob' and os_type is not null
) t
) t1 where rank =1
),
--gmv相关
rpt_core_monitor_gmv as (
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
ods_vova_vts.ods_vova_order_info_h oi
left join ods_vova_vts.ods_vova_order_relation_h ore on ore.order_id = oi.order_id
left join ods_vova_vts.ods_vova_region_h r on r.region_id = oi.country
where oi.pay_status >= 1
and oi.email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
and (oi.from_domain like '%vova%' or oi.from_domain like '%airyclub%')
and ore.device_type in (11,12)
and to_date(pay_time)='$pt' and pay_time <'$etime'
and oi.parent_order_id = 0
) t
left join rpt_core_monitor_app_version ap on ap.datasource = t.datasource and ap.device_id = t.device_id and ap.hour = t.hour
) t1 group by hour,datasource,os_type,region_code,app_version with cube
),
-- ctr相关
rpt_core_monitor_ctr as
(
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
select hour(collector_ts) hour,datasource,nvl(os_type,'NA') os_type, device_id click_device_id,null impression_device_id,1 clicks,0 impressions,nvl(app_version,'NA') app_version,nvl(geo_country,'NA') geo_country from dwd.dwd_vova_log_goods_click_arc where pt='$pt' and  collector_ts <'$etime'  and platform='mob'
union all
select hour(collector_ts) hour,datasource,nvl(os_type,'NA') os_type,device_id click_device_id,null impression_device_id,1 clicks,0 impressions,nvl(app_version,'NA') app_version,nvl(geo_country,'NA') geo_country from dwd.dwd_vova_log_click_arc where pt='$pt' and collector_ts < '$etime' and platform='mob' and event_type='goods'
union all
select hour(collector_ts) hour,datasource,nvl(os_type,'NA') os_type,null click_device_id,device_id impression_device_id,0 clicks,1 impressions,nvl(app_version,'NA') app_version,nvl(geo_country,'NA') geo_country from dwd.dwd_vova_log_goods_impression_arc where pt='$pt' and  collector_ts <'$etime'  and platform='mob'
union all
select hour(collector_ts) hour,datasource,nvl(os_type,'NA') os_type,null click_device_id,device_id impression_device_id,0 clicks,1 impressions,nvl(app_version,'NA') app_version,nvl(geo_country,'NA') geo_country from dwd.dwd_vova_log_impressions_arc where pt='$pt' and collector_ts < '$etime' and platform='mob' and event_type='goods'
) group by hour,datasource,os_type,app_version,geo_country with cube
),
rpt_core_monitor_res as (
select
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
from rpt_core_monitor_uv u
left join rpt_core_monitor_gmv g on u.hour = g.hour and u.datasource = g.datasource and u.os_type =g.os_type and u.app_version = g.app_version and u.geo_country = g.region_code
left join rpt_core_monitor_ctr c on u.hour = c.hour and u.datasource = c.datasource and u.os_type =c.os_type and u.app_version = c.app_version and u.geo_country = c.geo_country
where g.os_type!='NA'  and u.os_type!='NA' and c.os_type!='NA'
),
--获取最新的版本top5
rpt_core_monitor_app_version_top5 as (
select
nvl(datasource,'all') datasource,
nvl(hour,'25') hour,
nvl(app_version,'all') app_version
from
(
select
datasource,
hour,
app_version
from
(
select
datasource,
hour,
app_version,
row_number() over (partition by datasource,hour order by app_version desc) as rank
from
(
select
distinct
datasource,
hour,
app_version
from rpt_core_monitor_app_version where app_version not in ('1.9.0','1.8.0','1.7.0','1.6.0','1.5.0','1.3.0','1.2.0','2.7.0','2.8.0','2.9.0')
) t
) t1
where rank<=5
) group by datasource,hour,app_version with cube
),
rpt_core_monitor_base as (
select
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
from rpt_core_monitor_res t
left join tmp.tmp_vova_core_monitor_region_group rg on t.geo_country = rg.region_code
where t.geo_country != 'all' and rg.region_code is not null
group by t.hour,t.datasource,t.os_type,rg.region_group,t.app_version
union all
select
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
from rpt_core_monitor_res where geo_country = 'all'
)

insert overwrite table dwb.dwb_vova_core_monitor_base  PARTITION (pt = '$pt')
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
from rpt_core_monitor_base where app_version = 'all'
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
from rpt_core_monitor_base r
join rpt_core_monitor_app_version_top5 t on r.datasource = t.datasource and r.hour=t.hour and r.app_version = t.app_version
where r.app_version != 'all';

-- 环比计算
with rpt_core_monitor_mom as (
select
/*+ REPARTITION(1) */
hour,
datasource,
os_type,
region_group,
app_version,
(dau-last_dau)/last_dau dau_mom,
(gmv-last_gmv)/last_gmv gmv_mom,
(payed_order_num-last_payed_order_num)/last_payed_order_num payed_order_num_mom,
(payed_uv/dau)-(last_payed_uv/last_dau) payed_uv_div_dau_mom
from
(
select pt,hour,datasource,os_type,region_group,app_version,dau,lag(dau,1) over(partition by datasource,os_type,region_group,app_version order by pt,cast(hour as int)) last_dau,
gmv,lag(gmv,1) over(partition by datasource,os_type,region_group,app_version order by pt,cast(hour as int)) last_gmv,
payed_order_num,lag(payed_order_num,1) over(partition by datasource,os_type,region_group,app_version order by pt,cast(hour as int)) last_payed_order_num,
payed_uv,lag(payed_uv,1) over(partition by datasource,os_type,region_group,app_version order by pt,cast(hour as int)) last_payed_uv
from
(
select pt,hour,datasource,os_type,region_group,app_version,dau,gmv,payed_order_num,payed_uv from dwb.dwb_vova_core_monitor_base where pt ='$pt' and hour !=25
union all
select pt,hour,datasource,os_type,region_group,app_version,dau,gmv,payed_order_num,payed_uv from dwb.dwb_vova_core_monitor_base where pt ='$pre_pt' and hour =23
) t1
union all
select pt,hour,datasource,os_type,region_group,app_version,dau,lag(dau,1) over(partition by datasource,os_type,region_group,app_version order by pt) last_dau,
gmv,lag(gmv,1) over(partition by datasource,os_type,region_group,app_version order by pt) last_gmv,
payed_order_num,lag(payed_order_num,1) over(partition by datasource,os_type,region_group,app_version order by pt) last_payed_order_num,
payed_uv,lag(payed_uv,1) over(partition by datasource,os_type,region_group,app_version order by pt) last_payed_uv
from
(
select pt,hour,datasource,os_type,region_group,app_version,dau,gmv,payed_order_num,payed_uv from dwb.dwb_vova_core_monitor_base where pt ='$pt' and hour =25
union all
select pt,hour,datasource,os_type,region_group,app_version,dau,gmv,payed_order_num,payed_uv from dwb.dwb_vova_core_monitor_base where pt ='$pre_pt' and hour =25
) t2
) t2 where pt='$pt'
),

-- 同比计算
rpt_core_monitor_yoy as (
select
/*+ REPARTITION(1) */
t1.hour,
t1.datasource,
t1.os_type,
t1.region_group,
t1.app_version,
(t1.dau-t2.dau)/t2.dau dau_yoy,
(t1.gmv-t2.gmv)/t2.gmv gmv_yoy,
(t1.payed_order_num-t2.payed_order_num)/t2.payed_order_num payed_order_num_yoy,
(t1.payed_uv/t1.dau)-(t2.payed_uv/t2.dau) payed_uv_div_dau_yoy
from
(select hour,datasource,os_type,region_group,app_version,dau,gmv,payed_order_num,payed_uv from dwb.dwb_vova_core_monitor_base where pt ='$pt') t1
left join
(select hour,datasource,os_type,region_group,app_version,dau,gmv,payed_order_num,payed_uv from dwb.dwb_vova_core_monitor_base where pt ='$pre_pt') t2
on t1.hour = t2.hour and t1.datasource =t2.datasource and t1.os_type=t2.os_type and t1.region_group=t2.region_group and t1.app_version=t2.app_version
),

-- 7日均值
rpt_core_monitor_1w as (
select
/*+ REPARTITION(1) */
hour,datasource,os_type,region_group,app_version,avg(dau) dau_1w,avg(gmv) gmv_1w,avg(payed_order_num) payed_order_num_1w,avg(payed_uv/dau) payed_uv_dau_1w from dwb.dwb_vova_core_monitor_base where pt >='$pre_week' and pt<'$pt' group by hour,datasource,os_type,region_group,app_version
)

insert overwrite table dwb.dwb_vova_core_monitor  PARTITION (pt = '$pt')
select
/*+ REPARTITION(1) */
case when b.hour != 25 then from_unixtime(unix_timestamp(to_date('$pt'))+b.hour*60*60,'yyyy-MM-dd HH:00:00')
else  from_unixtime(unix_timestamp(to_date('$pt'))+23*60*60,'yyyy-MM-dd 23:59:59') end event_date,
b.hour,
b.datasource,
b.os_type,
b.region_group,
b.app_version,
nvl(b.dau,0) dau,
nvl(b.pd_uv,0) pd_uv,
nvl(b.cart_success_uv,0) cart_success_uv,
nvl(b.checkout_uv,0) checkout_uv,
nvl(b.payment_uv,0) payment_uv,
nvl(b.payment_confirm_uv,0) payment_confirm_uv,
nvl(b.gmv,0) gmv,
nvl(b.payed_order_num,0) payed_order_num,
nvl(b.payed_uv,0) payed_uv,
nvl(b.clicks,0) clicks,
nvl(b.impressions,0) impressions,
nvl(b.ctr,0) ctr,
nvl(b.click_uv,0) click_uv,
nvl(b.impression_uv,0) impression_uv,
nvl(m.dau_mom,0) dau_mom,
nvl(m.gmv_mom,0) gmv_mom,
nvl(m.payed_order_num_mom,0) payed_order_num_mom,
nvl(m.payed_uv_div_dau_mom,0) payed_uv_div_dau_mom,
nvl(y.dau_yoy,0) dau_yoy,
nvl(y.gmv_yoy,0) gmv_yoy,
nvl(y.payed_order_num_yoy,0) payed_order_num_yoy,
nvl(y.payed_uv_div_dau_yoy,0) payed_uv_div_dau_yoy,
nvl(w.dau_1w,0) dau_1w,
nvl(w.gmv_1w,0) gmv_1w,
nvl(w.payed_order_num_1w,0) payed_order_num_1w,
nvl(w.payed_uv_dau_1w,0) payed_uv_dau_1w,
nvl(b.payed_uv/b.dau,0) payed_uv_dau,
nvl(b.pd_uv/b.dau,0) pd_uv_dau,
nvl(b.cart_success_uv/b.pd_uv,0) cart_success_uv_pd_uv,
nvl(b.checkout_uv/b.cart_success_uv,0) checkout_uv_cart_success_uv,
nvl(b.payment_uv/b.checkout_uv,0) payment_uv_checkout_uv,
nvl(b.payment_confirm_uv/b.payment_uv,0) payment_confirm_uv_payment_uv,
nvl(b.payed_uv/b.payment_confirm_uv,0) payed_uv_payment_confirm_uv
from dwb.dwb_vova_core_monitor_base b
left join rpt_core_monitor_mom m on b.hour =m.hour and b.datasource= m.datasource and b.os_type = m.os_type and b.region_group=m.region_group and b.app_version=m.app_version
left join rpt_core_monitor_yoy y on b.hour =y.hour and b.datasource= y.datasource and b.os_type = y.os_type and b.region_group=y.region_group and b.app_version=y.app_version
left join rpt_core_monitor_1w w on b.hour =w.hour and b.datasource= w.datasource and b.os_type = w.os_type and b.region_group=w.region_group and b.app_version=w.app_version
where b.pt='$pt';
"
spark-sql  --conf "spark.app.name=dwb_vova_core_monitor_zhangyin"  --conf "spark.dynamicAllocation.maxExecutors=150" -e "$sql"
if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
--connect jdbc:mariadb:aurora://db-logistics-w.gitvv.com:3306/themis_logistics_report \
--username vvreport20210517 --password thuy*at1OhG1eiyoh8she \
--connection-manager org.apache.sqoop.manager.MySQLManager \
--table rpt_core_monitor_v2 \
--update-key "event_date,datasource,hour,os_type,region_group,app_version" \
--update-mode allowinsert \
--hcatalog-database dwb \
--hcatalog-table dwb_vova_core_monitor \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pt} \
--fields-terminated-by '\001'

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

spark-submit --master yarn \
--conf spark.dynamicAllocation.maxExecutors=20 \
--conf spark.app.name=alarm_system \
--conf spark.executor.memoryOverhead=2048 \
--jars  s3://vomkt-emr-rec/jar/vova-bd-monitor/javamail.jar  \
--class com.vova.bigdata.sparkbatch.monitor.MonitorMain s3://vomkt-emr-rec/jar/vova-bd-monitor/vova-bigdata-monitor-main.jar  \
--env product --db dwb --tlb dwb_vova_core_monitor --op check_index,send_message \
--date ${pt} --hour ${hour}

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
