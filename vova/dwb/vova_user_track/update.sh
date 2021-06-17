#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
pre10_day=`date -d $cur_date"-10 day" +%Y-%m-%d`
###逻辑sql

sql="
insert overwrite table dwb.dwb_expre_and_ord_his  PARTITION (pt = '${cur_date}')
select
t3.datasource,
t3.country,
t3.os_type,
t3.main_channel,
t3.is_new,
t3.dau,
t3.homepage_dau,
t3.pd_uv,
t3.cart_uv,
t3.cart_success_uv,
nvl(t5.ordered_user_num,0) ordered_user_num,
nvl(t4.payed_user_num,0) payed_user_num,
t3.checkout_uv,
t3.homepage_nav_uv,
t3.homepage_pop_uv,
t3.category_uv,
t3.sear_begin_uv,
t3.sear_result_uv,
t3.pro_list_uv,
t3.payment_uv,
t5.try_payment_uv
from
(
select
nvl(t2.datasource,'all') datasource,
nvl(t2.geo_country,'all') country,
nvl(t2.os_type,'all') os_type,
nvl(t2.main_channel,'all') main_channel,
nvl(t2.is_new,'all') is_new,
count(distinct t2.device_id) as dau,
count(distinct t2.homepage_device_id) as homepage_dau,
count(distinct t2.pd_device_id) as pd_uv,
count(distinct t2.cart_device_id) as cart_uv,
count(distinct t2.cart_success_device_id) as cart_success_uv,
count(distinct t2.checkout_device_id) as checkout_uv,
count(distinct t2.homepage_nav_device_id) as homepage_nav_uv,
count(distinct t2.homepage_pop_device_id) as homepage_pop_uv,
count(distinct t2.category_device_id) as category_uv,
count(distinct t2.sear_begin_device_id) as sear_begin_uv,
count(distinct t2.sear_result_device_id) as sear_result_uv,
count(distinct t2.pro_list_device_id) as pro_list_uv,
count(distinct t2.payment_device_id) as payment_uv
from
(
select
nvl(t1.datasource,'NA') datasource,
nvl(t1.geo_country,'NA') geo_country,
nvl(t1.os_type,'NA') os_type,
nvl(dd.main_channel,'NA') main_channel,
CASE WHEN datediff(t1.pt,dd.activate_time)=0 THEN 'new' ELSE 'old' END is_new,
CASE WHEN t1.event_name ='screen_view' THEN t1.device_id end device_id,
CASE when t1.page_code='homepage' and t1.event_name ='screen_view' THEN t1.device_id end homepage_device_id,
CASE when t1.page_code='product_detail' and t1.view_type='show' and t1.event_name ='screen_view' THEN t1.device_id end pd_device_id,
CASE when t1.page_code='cart' and t1.view_type='show' and t1.event_name ='screen_view' THEN t1.device_id end cart_device_id,
CASE when t1.page_code='product_detail' and t1.element_name='pdAddToCartSuccess' and t1.event_name ='common_click' THEN t1.device_id end cart_success_device_id,
CASE when t1.page_code like '%checkout%' and t1.view_type='show' and t1.event_name ='screen_view' THEN t1.device_id  end checkout_device_id,
CASE when t1.page_code='homepage'  and t1.event_name ='impressions' and  list_type like 'hp_topNavigation%' THEN t1.device_id end homepage_nav_device_id,
CASE when t1.page_code='homepage' and  t1.event_name ='goods_impression' and  list_type='/product_list_popular' THEN t1.device_id end homepage_pop_device_id,
CASE when t1.page_code='category' and t1.view_type='show' and t1.event_name ='screen_view'   THEN t1.device_id end category_device_id,
CASE when t1.page_code='search_begin' and t1.view_type='show' and t1.event_name ='screen_view'  THEN t1.device_id end sear_begin_device_id,
CASE when t1.page_code='search_result' and t1.view_type='show' and t1.event_name ='screen_view'  THEN t1.device_id end sear_result_device_id,
CASE when t1.page_code='product_list' and t1.view_type='show' and t1.event_name ='screen_view'  THEN t1.device_id end pro_list_device_id,
CASE when t1.page_code='payment'  THEN t1.device_id end payment_device_id

from
(
select pt,datasource,event_name,geo_country,os_type,page_code,device_id,referrer,view_type,NULL element_name,NULL list_type from dwd.dwd_vova_log_screen_view where pt='${cur_date}' and platform ='mob' and os_type is not null and os_type !='' and device_id is not null
union all
select pt,datasource,event_name,geo_country,os_type,page_code,device_id,referrer,NULL view_type,element_name, NULL list_type from dwd.dwd_vova_log_common_click where pt='${cur_date}' and platform ='mob' and os_type is not null and os_type !='' and device_id is not null
union all
select pt,datasource,event_name,geo_country,os_type,page_code,device_id,referrer,NULL view_type,NULL element_name,list_type from dwd.dwd_vova_log_goods_impression where pt='${cur_date}' and platform ='mob' and os_type is not null and os_type !='' and device_id is not null
union all
select pt,datasource,event_name,geo_country,os_type,page_code,device_id,referrer,NULL view_type,NULL element_name,list_type from dwd.dwd_vova_log_impressions where pt='${cur_date}' and platform ='mob' and os_type is not null and os_type !='' and device_id is not null
) t1
left join dim.dim_vova_devices dd on dd.device_id = t1.device_id and dd.datasource=t1.datasource
) t2
group by
   t2.datasource,
   t2.geo_country,
   t2.os_type,
   t2.main_channel,
   t2.is_new
  with cube
) t3 left join
(
select
nvl(t1.datasource,'all')  datasource,
nvl(t1.region_code,'all') region_code,
nvl(t1.platform,'all') platform,
nvl(t1.main_channel,'all') main_channel,
nvl(t1.is_new,'all') is_new,
count(distinct t1.buyer_id) as payed_user_num
from
(
select
nvl(fp.datasource,'NA') datasource,
nvl(fp.region_code,'NA') region_code,
nvl(fp.platform,'NA') platform,
nvl(dd.main_channel,'NA') main_channel,
CASE WHEN datediff(fp.pay_time,dd.activate_time)=0 THEN 'new' ELSE 'old' END is_new,
fp.buyer_id
from
dwd.dwd_vova_fact_pay fp
left join dim.dim_vova_devices dd on dd.device_id = fp.device_id and dd.datasource=fp.datasource
inner join dim.dim_vova_order_goods ddog on ddog.order_goods_id = fp.order_goods_id
where to_date(fp.pay_time)='${cur_date}' and (fp.from_domain like '%api.vova%' or fp.from_domain like '%api.airyclub%')
and (ddog.order_tag not like '%luckystar_activity_id%' or ddog.order_tag is null)
) t1
group by
   t1.datasource,
   t1.region_code,
   t1.platform,
   t1.main_channel,
   t1.is_new
  with cube
)t4 on t3.datasource=t4.datasource and t3.country=t4.region_code and t3.os_type=t4.platform and t3.main_channel=t4.main_channel and t3.is_new=t4.is_new
left join
(
select
nvl(t1.datasource,'all') datasource,
nvl(t1.region_code,'all') region_code,
nvl(t1.platform,'all') platform,
nvl(t1.main_channel,'all') main_channel,
nvl(t1.is_new,'all') is_new,
count(distinct t1.buyer_id) as ordered_user_num,
count(distinct if(order_sn is not null, t1.buyer_id, null) ) as try_payment_uv
from
(
select
nvl(ddog.datasource,'NA') datasource,
nvl(ddog.region_code,'NA') region_code,
nvl(ddog.platform,'NA') platform,
nvl(dd.main_channel,'NA') main_channel,
pt.order_sn,
CASE WHEN datediff(ddog.order_time,dd.activate_time)=0 THEN 'new' ELSE 'old' END is_new,
ddog.buyer_id
from dim.dim_vova_order_goods ddog
left join dim.dim_vova_devices dd on dd.device_id = ddog.device_id and dd.datasource=ddog.datasource
LEFT JOIN (SELECT order_sn  FROM ods_vova_vts.ods_vova_paypal_txn where date(txn_datetime)>=date_add('${cur_date}',-7) group by order_sn) pt ON pt.order_sn = ddog.order_sn
where to_date(ddog.order_time) = '${cur_date}' and (ddog.from_domain like '%api.vova%' or ddog.from_domain like '%api.airyclub%')
and (ddog.order_tag not like '%luckystar_activity_id%' or ddog.order_tag is null)
) t1
group by
   t1.datasource,
   t1.region_code,
   t1.platform,
   t1.main_channel,
   t1.is_new
  with cube
) t5 on t3.datasource=t5.datasource and t3.country=t5.region_code and t3.os_type=t5.platform and t3.main_channel=t5.main_channel and t3.is_new=t5.is_new
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 5G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=dwb_expre_and_ord_his" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

