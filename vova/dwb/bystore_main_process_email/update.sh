#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

sql="
insert overwrite table dwb.dwb_vova_bystore_main_process  PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
t3.is_new,
t3.dau,
t3.homepage_dau,
nvl(t3.first_order_num,0) first_order_num,
nvl(t4.payed_user_num,0) payed_user_num,
nvl(t4.payed_order_num,0) payed_order_num,
nvl(t4.gmv,0) gmv,
nvl(t5.ordered_user_num,0) ordered_user_num,
nvl(t5.ordered_order_num,0) ordered_order_num,
nvl(t4.brand_gmv,0) brand_gmv,
nvl(t4.no_brand_gmv,0) no_brand_gmv
from
(
select
nvl(t2.is_new,'all') is_new,
count(distinct t2.device_id) as dau,
count(distinct t2.homepage_device_id) as homepage_dau,
count(distinct t2.first_order_id) as first_order_num
from
(
select
CASE WHEN datediff(t1.pt,dd.activate_time)<=0 THEN 'new'
  WHEN datediff(t1.pt,dd.activate_time)>=1 and datediff(t1.pt,dd.activate_time)<6 THEN '2-7'
  WHEN datediff(t1.pt,dd.activate_time)>=7 and datediff(t1.pt,dd.activate_time)<29 THEN '8-30'
  else '30+' END is_new,
CASE WHEN t1.event_name ='screen_view' THEN t1.device_id end device_id,
CASE when datediff(t1.pt,dd.first_pay_time)=0 THEN dd.first_order_id END first_order_id,
CASE when t1.page_code='homepage' and t1.event_name ='screen_view' THEN t1.device_id end homepage_device_id
from
(
select pt,datasource,event_name,geo_country,os_type,page_code,device_id,referrer,view_type,NULL element_name,NULL list_name from dwd.dwd_vova_log_screen_view where pt='${cur_date}' and dp='others' and  datasource='bystore' and platform ='mob' and os_type is not null and os_type !='' and device_id is not null
union all
select pt,datasource,event_name,geo_country,os_type,page_code,device_id,referrer,NULL view_type,element_name,list_name from dwd.dwd_vova_log_common_click where pt='${cur_date}' and dp='others' and  datasource='bystore' and platform ='mob' and os_type is not null and os_type !='' and device_id is not null
union all
select pt,datasource,event_name,geo_country,os_type,page_code,device_id,referrer,NULL view_type,element_name,NULL list_name from dwd.dwd_vova_log_order_process where pt='${cur_date}' and dp='others' and  datasource='bystore' and platform ='mob' and os_type is not null and os_type !='' and device_id is not null
) t1
left join dim.dim_vova_devices dd on dd.device_id = t1.device_id and dd.datasource=t1.datasource
) t2
group by t2.is_new with cube
) t3 left join
(
select
nvl(t1.is_new,'all') is_new,
count(distinct t1.buyer_id) as payed_user_num,
count(distinct t1.order_id) as payed_order_num,
sum(t1.shop_price * t1.goods_number + t1.shipping_fee) as gmv,
sum(brand_gmv) brand_gmv,
sum(no_brand_gmv) no_brand_gmv
from
(
select
CASE WHEN datediff(fp.pay_time,dd.activate_time)<=0 THEN 'new'
  WHEN datediff(fp.pay_time,dd.activate_time)>=1 and datediff(fp.pay_time,dd.activate_time)<6 THEN '2-7'
  WHEN datediff(fp.pay_time,dd.activate_time)>=7 and datediff(fp.pay_time,dd.activate_time)<29 THEN '8-30'
  else '30+' END is_new,
fp.order_id,
fp.buyer_id,
fp.device_id,
fp.shop_price,
fp.shipping_fee,
fp.goods_number,
case when ddog.brand_id>0 then fp.shop_price * fp.goods_number + fp.shipping_fee else 0 end brand_gmv,
case when ddog.brand_id=0 then fp.shop_price * fp.goods_number + fp.shipping_fee else 0 end no_brand_gmv
from
dwd.dwd_vova_fact_pay fp
left join dim.dim_vova_devices dd on dd.device_id = fp.device_id and dd.datasource=fp.datasource
inner join dim.dim_vova_order_goods ddog on ddog.order_goods_id = fp.order_goods_id
where to_date(fp.pay_time)='${cur_date}' and fp.from_domain like '%api%' and fp.datasource='floryhub'
and (ddog.order_tag not like '%luckystar_activity_id%' or ddog.order_tag is null)
) t1
group by t1.is_new with cube
)t4 on  t3.is_new=t4.is_new
left join
(
select
nvl(t1.is_new,'all') is_new,
count(distinct t1.buyer_id) as ordered_user_num,
count(distinct t1.order_id) as ordered_order_num
from
(
select
CASE WHEN datediff(ddog.order_time,dd.activate_time)<=0 THEN 'new'
  WHEN datediff(ddog.order_time,dd.activate_time)>=1 and datediff(ddog.order_time,dd.activate_time)<6 THEN '2-7'
  WHEN datediff(ddog.order_time,dd.activate_time)>=7 and datediff(ddog.order_time,dd.activate_time)<29 THEN '8-30'
  else '30+' END is_new,
ddog.order_id,
ddog.buyer_id,
ddog.device_id
from dim.dim_vova_order_goods ddog
left join dim.dim_vova_devices dd on dd.device_id = ddog.device_id and dd.datasource=ddog.datasource
where to_date(ddog.order_time) = '${cur_date}' and ddog.from_domain like '%api%' and ddog.datasource='floryhub'
and (ddog.order_tag not like '%luckystar_activity_id%' or ddog.order_tag is null)
) t1
group by t1.is_new with cube
) t5 on t3.is_new=t5.is_new
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql --conf "spark.app.name=dwb_vova_bystore_main_process_zhangyin" --conf "spark.dynamicAllocation.maxExecutors=100" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
