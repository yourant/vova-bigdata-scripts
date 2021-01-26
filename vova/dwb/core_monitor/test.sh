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
-- 环比计算
drop table if exists tmp.rpt_core_monitor_mom_v2;
create table tmp.rpt_core_monitor_mom_v2 as
select
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
select pt,hour,datasource,os_type,region_group,app_version,dau,gmv,payed_order_num,payed_uv from rpt.rpt_core_monitor_base_v2 where pt ='$pt' and hour !=25
union all
select pt,hour,datasource,os_type,region_group,app_version,dau,gmv,payed_order_num,payed_uv from rpt.rpt_core_monitor_base_v2 where pt ='$pre_pt' and hour =23
) t1
union all
select pt,hour,datasource,os_type,region_group,app_version,dau,lag(dau,1) over(partition by datasource,os_type,region_group,app_version order by pt) last_dau,
gmv,lag(gmv,1) over(partition by datasource,os_type,region_group,app_version order by pt) last_gmv,
payed_order_num,lag(payed_order_num,1) over(partition by datasource,os_type,region_group,app_version order by pt) last_payed_order_num,
payed_uv,lag(payed_uv,1) over(partition by datasource,os_type,region_group,app_version order by pt) last_payed_uv
from
(
select pt,hour,datasource,os_type,region_group,app_version,dau,gmv,payed_order_num,payed_uv from rpt.rpt_core_monitor_base_v2 where pt ='$pt' and hour =25
union all
select pt,hour,datasource,os_type,region_group,app_version,dau,gmv,payed_order_num,payed_uv from rpt.rpt_core_monitor_base_v2 where pt ='$pre_pt' and hour =25
) t2
) t2 where pt='$pt';

-- 同比计算
drop table if exists tmp.rpt_core_monitor_yoy_v2;
create table tmp.rpt_core_monitor_yoy_v2 as
select
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
(select hour,datasource,os_type,region_group,app_version,dau,gmv,payed_order_num,payed_uv from rpt.rpt_core_monitor_base_v2 where pt ='$pt') t1
left join
(select hour,datasource,os_type,region_group,app_version,dau,gmv,payed_order_num,payed_uv from rpt.rpt_core_monitor_base_v2 where pt ='$pre_pt') t2
on t1.hour = t2.hour and t1.datasource =t2.datasource and t1.os_type=t2.os_type and t1.region_group=t2.region_group and t1.app_version=t2.app_version;

-- 7日均值
drop table if exists tmp.rpt_core_monitor_1w_v2;
create table tmp.rpt_core_monitor_1w_v2 as
select hour,datasource,os_type,region_group,app_version,avg(dau) dau_1w,avg(gmv) gmv_1w,avg(payed_order_num) payed_order_num_1w,avg(payed_uv/dau) payed_uv_dau_1w from rpt.rpt_core_monitor_base_v2 where pt >='$pre_week' group by hour,datasource,os_type,region_group,app_version;

insert overwrite table rpt.rpt_core_monitor_v2  PARTITION (pt = '$pt')
select
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
nvl(w.payed_uv_dau_1w,0) payed_uv_dau_1w
from rpt.rpt_core_monitor_base_v2 b
left join tmp.rpt_core_monitor_mom_v2 m on b.hour =m.hour and b.datasource= m.datasource and b.os_type = m.os_type and b.region_group=m.region_group and b.app_version=m.app_version
left join tmp.rpt_core_monitor_yoy_v2 y on b.hour =y.hour and b.datasource= y.datasource and b.os_type = y.os_type and b.region_group=y.region_group and b.app_version=y.app_version
left join tmp.rpt_core_monitor_1w_v2 w on b.hour =w.hour and b.datasource= w.datasource and b.os_type = w.os_type and b.region_group=w.region_group and b.app_version=w.app_version
where b.pt='$pt';
"
spark-sql --conf "spark.app.name=rpt_core_monitor_test" -e "$sql"
if [ $? -ne 0 ];then
  exit 1
fi