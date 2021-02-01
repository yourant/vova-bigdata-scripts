#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-0 day" +%Y-%m-%d`
fi
###逻辑sql

sql="
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE dwb.dwb_zq_main_process PARTITION (domain_group='FN', pt)
select
/*+ REPARTITION(1) */
fin.event_date,
fin.datasource,
fin.region_code,
fin.platform,
fin.user_source,
sum(fin.dau) as dau,
sum(fin.home_page_uv) as home_page_uv,
sum(fin.cart_uv) as cart_uv,
sum(fin.checkout_uv) as checkout_uv,
sum(fin.product_detail_uv) as product_detail_uv,
sum(fin.add_cart_success_uv) as add_cart_success_uv,
sum(fin.continue_checkout_uv) as continue_checkout_uv,
sum(fin.gmv) as gmv,
sum(fin.paid_uv) as paid_uv,
sum(fin.paid_order_cnt) as paid_order_cnt,
sum(fin.first_order_cnt) as first_order_cnt,
fin.is_new,
fin.hour,
fin.event_date AS pt
from
(
select
nvl(log_data.datasource,'all') datasource,
nvl(log_data.pt,'all') event_date,
nvl(log_data.hour,'all') hour,
nvl(log_data.region_code,'all') region_code,
nvl(log_data.platform,'all') platform,
nvl(log_data.user_source,'all') user_source,
nvl(nvl(log_data.is_new,'new'),'all') is_new,
count(distinct log_data.activate_domain_userId) as dau,
count(distinct log_data.homepage_domain_userId) as home_page_uv,
count(distinct log_data.cart_domain_userId) as cart_uv,
count(distinct log_data.check_out_domain_userId) as checkout_uv,
count(distinct log_data.product_detail_domain_userId) as product_detail_uv,
count(distinct log_data.add_cart_success_domain_userId) as add_cart_success_uv,
count(distinct log_data.continue_checkout_domain_userId) as continue_checkout_uv,
0 as gmv,
0 as paid_uv,
0 as paid_order_cnt,
0 as first_order_cnt
from
(
select
nvl(t1.datasource,'NA') datasource,
nvl(t1.geo_country,'NA') region_code,
nvl(t1.platform,'NA') platform,
t1.pt,
t1.hour,
CASE WHEN datediff(t1.pt,fdu.activate_time)<=0 THEN 'new'
  WHEN datediff(t1.pt,fdu.activate_time)>=1 and datediff(t1.pt,fdu.activate_time)<6 THEN '2-7'
  WHEN datediff(t1.pt,fdu.activate_time)>=7 and datediff(t1.pt,fdu.activate_time)<29 THEN '8-30'
  WHEN fdu.activate_time IS NULL THEN 'new'
  else '30+' END is_new,
nvl(fdu.original_channel, 'unknown') AS user_source,
CASE when t1.event_name ='page_view' and t1.page_code='homepage' THEN t1.domain_userId end homepage_domain_userId,
CASE when t1.event_name ='page_view' THEN t1.domain_userId end activate_domain_userId,
CASE when t1.event_name ='page_view' and  t1.page_code='cart' THEN t1.domain_userId end cart_domain_userId,
CASE when t1.event_name ='page_view' and  t1.page_code='check_out' THEN t1.domain_userId end check_out_domain_userId,
CASE when t1.event_name ='page_view' and  t1.page_code='product_detail' THEN t1.domain_userId end product_detail_domain_userId,
CASE when t1.event_name ='data' and t1.page_code='product_detail' and t1.element_name = 'AddToCartSuccess' THEN t1.domain_userId end add_cart_success_domain_userId,
CASE when t1.event_name ='click' and t1.page_code='check_out' and t1.element_name = 'continue_checkout' THEN t1.domain_userId end continue_checkout_domain_userId
from
(
select pt,hour,datasource,domain_userId,platform,buyer_id,event_name,geo_country,os_type,page_code,device_id,referrer,view_type,NULL element_name,NULL list_name from dwd.dwd_vova_log_page_view_arc where pt>=date_sub('${cur_date}', 30) and pt<='${cur_date}' and datasource NOT IN ('vova')
union all
select pt,hour,datasource,domain_userId,platform,buyer_id,event_name,geo_country,os_type,page_code,device_id,referrer,NULL view_type,element_name,NULL list_name from dwd.dwd_vova_log_data_arc where pt>=date_sub('${cur_date}', 30) and pt<='${cur_date}' and datasource NOT IN ('vova')
union all
select pt,hour,datasource,domain_userId,platform,buyer_id,event_name,geo_country,os_type,page_code,device_id,referrer,NULL view_type,element_name,NULL list_name from dwd.dwd_vova_log_click_arc where pt>=date_sub('${cur_date}', 30) and pt<='${cur_date}' and datasource NOT IN ('vova') and event_type='normal'
) t1
LEFT JOIN dim.dim_zq_domain_userid fdu on fdu.domain_userid = t1.domain_userid AND fdu.datasource = t1.datasource
INNER JOIN dim.dim_zq_site zs on zs.datasource = t1.datasource AND zs.domain_group = 'FN'
) log_data
group by cube(log_data.datasource, log_data.region_code, log_data.platform, log_data.user_source, log_data.pt, log_data.hour, nvl(log_data.is_new,'new'))
UNION ALL
select
nvl(nvl(oi.project_name,'NA'),'all') AS datasource,
nvl(date(oi.pay_time),'all') AS event_date,
nvl(if(HOUR(oi.pay_time)<10,concat('0', HOUR(oi.pay_time)),HOUR(oi.pay_time)),'all') AS hour,
nvl(nvl(r.region_code,'NA') ,'all') region_code,
nvl(if(oi.from_domain like '%api%', 'web', 'pc'),'all') platform,
nvl(nvl(fdu.original_channel, 'unknown'), 'all') AS user_source,
nvl(nvl(CASE WHEN datediff(date(oi.pay_time),fdu.activate_time)<=0 THEN 'new'
  WHEN datediff(date(oi.pay_time),fdu.activate_time)>=1 and datediff(date(oi.pay_time),fdu.activate_time)<6 THEN '2-7'
  WHEN datediff(date(oi.pay_time),fdu.activate_time)>=7 and datediff(date(oi.pay_time),fdu.activate_time)<29 THEN '8-30'
  WHEN fdu.activate_time IS NULL THEN 'new'
  else '30+' END, 'new'), 'all') AS is_new,
0 AS dau,
0 as home_page_uv,
0 as cart_uv,
0 as checkout_uv,
0 as product_detail_uv,
0 as checkout_click_success_uv,
0 as continue_checkout_uv,
sum(oi.goods_amount + oi.shipping_fee) AS gmv,
count(DISTINCT oi.user_id) AS paid_uv,
count(*) AS paid_order_cnt,
count(if(pre_order.min_order_id is not null, oi.order_id, null)) AS first_order_cnt
from
ods_zq_zsp.ods_zq_order_info_h oi
INNER JOIN dim.dim_zq_site zs on zs.datasource = oi.project_name AND zs.domain_group = 'FN'
LEFT JOIN ods_zq_zsp.ods_zq_region r on r.region_id = oi.country
LEFT JOIN
(
select
datasource,
cur_buyer_id,
first(original_channel) as original_channel,
first(activate_time) as activate_time
from
dim.dim_zq_domain_userid fdu
group by cur_buyer_id, datasource
) fdu on fdu.cur_buyer_id = oi.user_id AND fdu.datasource = oi.project_name
LEFT JOIN (
SELECT oi.user_id,min(oi.order_id) AS min_order_id, oi.project_name
FROM ods_zq_zsp.ods_zq_order_info_h oi
WHERE oi.pay_status >= 1
group by oi.user_id, oi.project_name
) pre_order on pre_order.min_order_id = oi.order_id AND oi.project_name = pre_order.project_name
WHERE date(oi.pay_time) >= date_sub('${cur_date}', 30)
AND date(oi.pay_time) <= '${cur_date}'
AND oi.pay_status >= 1
GROUP BY CUBE (if(oi.from_domain like '%api%', 'web', 'pc'), date(oi.pay_time), nvl(r.region_code,'NA'), nvl(oi.project_name,'NA'),
nvl(CASE WHEN datediff(date(oi.pay_time),fdu.activate_time)<=0 THEN 'new'
  WHEN datediff(date(oi.pay_time),fdu.activate_time)>=1 and datediff(date(oi.pay_time),fdu.activate_time)<6 THEN '2-7'
  WHEN datediff(date(oi.pay_time),fdu.activate_time)>=7 and datediff(date(oi.pay_time),fdu.activate_time)<29 THEN '8-30'
  WHEN fdu.activate_time IS NULL THEN 'new'
  else '30+' END, 'new'), if(HOUR(oi.pay_time)<10,concat('0', HOUR(oi.pay_time)),HOUR(oi.pay_time)),
nvl(fdu.original_channel, 'unknown'))
) fin
group by fin.event_date, fin.region_code, fin.platform, fin.user_source, fin.is_new, fin.datasource, fin.hour
having event_date != 'all'
;

INSERT OVERWRITE TABLE dwb.dwb_zq_main_process PARTITION (domain_group='FD', pt)
select
/*+ REPARTITION(1) */
fin.event_date,
fin.datasource,
fin.region_code,
fin.platform,
fin.user_source,
sum(fin.dau) as dau,
sum(fin.home_page_uv) as home_page_uv,
sum(fin.cart_uv) as cart_uv,
sum(fin.checkout_uv) as checkout_uv,
sum(fin.product_detail_uv) as product_detail_uv,
sum(fin.add_cart_success_uv) as add_cart_success_uv,
sum(fin.continue_checkout_uv) as continue_checkout_uv,
sum(fin.gmv) as gmv,
sum(fin.paid_uv) as paid_uv,
sum(fin.paid_order_cnt) as paid_order_cnt,
sum(fin.first_order_cnt) as first_order_cnt,
fin.is_new,
fin.hour,
fin.event_date AS pt
from
(
select
nvl(log_data.datasource,'all') datasource,
nvl(log_data.pt,'all') event_date,
nvl(log_data.hour,'all') hour,
nvl(log_data.region_code,'all') region_code,
nvl(log_data.platform,'all') platform,
nvl(log_data.user_source,'all') user_source,
nvl(nvl(log_data.is_new,'new'),'all') is_new,
count(distinct log_data.activate_domain_userId) as dau,
count(distinct log_data.homepage_domain_userId) as home_page_uv,
count(distinct log_data.cart_domain_userId) as cart_uv,
count(distinct log_data.check_out_domain_userId) as checkout_uv,
count(distinct log_data.product_detail_domain_userId) as product_detail_uv,
count(distinct log_data.add_cart_success_domain_userId) as add_cart_success_uv,
count(distinct log_data.continue_checkout_domain_userId) as continue_checkout_uv,
0 as gmv,
0 as paid_uv,
0 as paid_order_cnt,
0 as first_order_cnt
from
(
select
nvl(t1.datasource,'NA') datasource,
nvl(t1.geo_country,'NA') region_code,
nvl(t1.platform,'NA') platform,
t1.pt,
t1.hour,
CASE WHEN datediff(t1.pt,fdu.activate_time)<=0 THEN 'new'
  WHEN datediff(t1.pt,fdu.activate_time)>=1 and datediff(t1.pt,fdu.activate_time)<6 THEN '2-7'
  WHEN datediff(t1.pt,fdu.activate_time)>=7 and datediff(t1.pt,fdu.activate_time)<29 THEN '8-30'
  WHEN fdu.activate_time IS NULL THEN 'new'
  else '30+' END is_new,
nvl(fdu.original_channel, 'unknown') AS user_source,
CASE when t1.event_name ='page_view' and t1.page_code='homepage' THEN t1.domain_userId end homepage_domain_userId,
CASE when t1.event_name ='page_view' THEN t1.domain_userId end activate_domain_userId,
CASE when t1.event_name ='page_view' and  t1.page_code='cart' THEN t1.domain_userId end cart_domain_userId,
CASE when t1.event_name ='page_view' and  t1.page_code='check_out' THEN t1.domain_userId end check_out_domain_userId,
CASE when t1.event_name ='page_view' and  t1.page_code='product_detail' THEN t1.domain_userId end product_detail_domain_userId,
CASE when t1.event_name ='data' and t1.page_code='product_detail' and t1.element_name = 'AddToCartSuccess' THEN t1.domain_userId end add_cart_success_domain_userId,
CASE when t1.event_name ='click' and t1.page_code='check_out' and t1.element_name = 'continue_checkout' THEN t1.domain_userId end continue_checkout_domain_userId
from
(
select pt,hour,datasource,domain_userId,platform,buyer_id,event_name,geo_country,os_type,page_code,device_id,referrer,view_type,NULL element_name,NULL list_name from dwd.dwd_vova_log_page_view_arc where pt>=date_sub('${cur_date}', 30) and pt<='${cur_date}' and datasource NOT IN ('vova')
union all
select pt,hour,datasource,domain_userId,platform,buyer_id,event_name,geo_country,os_type,page_code,device_id,referrer,NULL view_type,element_name,NULL list_name from dwd.dwd_vova_log_data_arc where pt>=date_sub('${cur_date}', 30) and pt<='${cur_date}' and datasource NOT IN ('vova')
union all
select pt,hour,datasource,domain_userId,platform,buyer_id,event_name,geo_country,os_type,page_code,device_id,referrer,NULL view_type,element_name,NULL list_name from dwd.dwd_vova_log_click_arc where pt>=date_sub('${cur_date}', 30) and pt<='${cur_date}' and datasource NOT IN ('vova') and event_type='normal'
) t1
LEFT JOIN dim.dim_zq_domain_userid fdu on fdu.domain_userid = t1.domain_userid AND fdu.datasource = t1.datasource
INNER JOIN dim.dim_zq_site zs on zs.datasource = t1.datasource AND zs.domain_group = 'FD'
) log_data
group by cube(log_data.datasource, log_data.region_code, log_data.platform, log_data.user_source, log_data.pt, log_data.hour, nvl(log_data.is_new,'new'))
UNION ALL
select
nvl(nvl(oi.project_name,'NA'),'all') AS datasource,
nvl(date(oi.pay_time),'all') AS event_date,
nvl(if(HOUR(oi.pay_time)<10,concat('0', HOUR(oi.pay_time)),HOUR(oi.pay_time)),'all') AS hour,
nvl(nvl(r.region_code,'NA') ,'all') region_code,
nvl(if(oi.from_domain like '%api%', 'web', 'pc'),'all') platform,
nvl(nvl(fdu.original_channel, 'unknown'), 'all') AS user_source,
nvl(nvl(CASE WHEN datediff(date(oi.pay_time),fdu.activate_time)<=0 THEN 'new'
  WHEN datediff(date(oi.pay_time),fdu.activate_time)>=1 and datediff(date(oi.pay_time),fdu.activate_time)<6 THEN '2-7'
  WHEN datediff(date(oi.pay_time),fdu.activate_time)>=7 and datediff(date(oi.pay_time),fdu.activate_time)<29 THEN '8-30'
  WHEN fdu.activate_time IS NULL THEN 'new'
  else '30+' END, 'new'), 'all') AS is_new,
0 AS dau,
0 as home_page_uv,
0 as cart_uv,
0 as checkout_uv,
0 as product_detail_uv,
0 as checkout_click_success_uv,
0 as continue_checkout_uv,
sum(oi.goods_amount + oi.shipping_fee) AS gmv,
count(DISTINCT oi.user_id) AS paid_uv,
count(*) AS paid_order_cnt,
count(if(pre_order.min_order_id is not null, oi.order_id, null)) AS first_order_cnt
from
ods_zq_zsp.ods_zq_order_info_h oi
INNER JOIN dim.dim_zq_site zs on zs.datasource = oi.project_name AND zs.domain_group = 'FD'
LEFT JOIN ods_zq_zsp.ods_zq_region r on r.region_id = oi.country
LEFT JOIN
(
select
datasource,
cur_buyer_id,
first(original_channel) as original_channel,
first(activate_time) as activate_time
from
dim.dim_zq_domain_userid fdu
group by cur_buyer_id, datasource
) fdu on fdu.cur_buyer_id = oi.user_id AND fdu.datasource = oi.project_name
LEFT JOIN (
SELECT oi.user_id,min(oi.order_id) AS min_order_id, oi.project_name
FROM ods_zq_zsp.ods_zq_order_info_h oi
WHERE oi.pay_status >= 1
group by oi.user_id, oi.project_name
) pre_order on pre_order.min_order_id = oi.order_id AND oi.project_name = pre_order.project_name
WHERE date(oi.pay_time) >= date_sub('${cur_date}', 30)
AND date(oi.pay_time) <= '${cur_date}'
AND oi.pay_status >= 1
GROUP BY CUBE (if(oi.from_domain like '%api%', 'web', 'pc'), date(oi.pay_time), nvl(r.region_code,'NA'), nvl(oi.project_name,'NA'),
nvl(CASE WHEN datediff(date(oi.pay_time),fdu.activate_time)<=0 THEN 'new'
  WHEN datediff(date(oi.pay_time),fdu.activate_time)>=1 and datediff(date(oi.pay_time),fdu.activate_time)<6 THEN '2-7'
  WHEN datediff(date(oi.pay_time),fdu.activate_time)>=7 and datediff(date(oi.pay_time),fdu.activate_time)<29 THEN '8-30'
  WHEN fdu.activate_time IS NULL THEN 'new'
  else '30+' END, 'new'),if(HOUR(oi.pay_time)<10,concat('0', HOUR(oi.pay_time)),HOUR(oi.pay_time)),
nvl(fdu.original_channel, 'unknown'))
) fin
group by fin.event_date, fin.region_code, fin.platform, fin.user_source, fin.is_new, fin.datasource, fin.hour
having event_date != 'all' AND hour = 'all'
;


INSERT OVERWRITE TABLE dwb.dwb_zq_main_process PARTITION (domain_group='TRIGRAM', pt)
select
/*+ REPARTITION(1) */
fin.event_date,
fin.datasource,
fin.region_code,
fin.platform,
fin.user_source,
sum(fin.dau) as dau,
sum(fin.home_page_uv) as home_page_uv,
sum(fin.cart_uv) as cart_uv,
sum(fin.checkout_uv) as checkout_uv,
sum(fin.product_detail_uv) as product_detail_uv,
sum(fin.add_cart_success_uv) as add_cart_success_uv,
sum(fin.continue_checkout_uv) as continue_checkout_uv,
sum(fin.gmv) as gmv,
sum(fin.paid_uv) as paid_uv,
sum(fin.paid_order_cnt) as paid_order_cnt,
sum(fin.first_order_cnt) as first_order_cnt,
fin.is_new,
fin.hour,
fin.event_date AS pt
from
(
select
nvl(log_data.datasource,'all') datasource,
nvl(log_data.pt,'all') event_date,
nvl(log_data.hour,'all') hour,
nvl(log_data.region_code,'all') region_code,
nvl(log_data.platform,'all') platform,
nvl(log_data.user_source,'all') user_source,
nvl(nvl(log_data.is_new,'new'),'all') is_new,
count(distinct log_data.activate_domain_userId) as dau,
count(distinct log_data.homepage_domain_userId) as home_page_uv,
count(distinct log_data.cart_domain_userId) as cart_uv,
count(distinct log_data.check_out_domain_userId) as checkout_uv,
count(distinct log_data.product_detail_domain_userId) as product_detail_uv,
count(distinct log_data.add_cart_success_domain_userId) as add_cart_success_uv,
count(distinct log_data.continue_checkout_domain_userId) as continue_checkout_uv,
0 as gmv,
0 as paid_uv,
0 as paid_order_cnt,
0 as first_order_cnt
from
(
select
nvl(t1.datasource,'NA') datasource,
nvl(t1.geo_country,'NA') region_code,
nvl(t1.platform,'NA') platform,
t1.pt,
t1.hour,
CASE WHEN datediff(t1.pt,fdu.activate_time)<=0 THEN 'new'
  WHEN datediff(t1.pt,fdu.activate_time)>=1 and datediff(t1.pt,fdu.activate_time)<6 THEN '2-7'
  WHEN datediff(t1.pt,fdu.activate_time)>=7 and datediff(t1.pt,fdu.activate_time)<29 THEN '8-30'
  WHEN fdu.activate_time IS NULL THEN 'new'
  else '30+' END is_new,
nvl(fdu.original_channel, 'unknown') AS user_source,
CASE when t1.event_name ='page_view' and t1.page_code='homepage' THEN t1.domain_userId end homepage_domain_userId,
CASE when t1.event_name ='page_view' THEN t1.domain_userId end activate_domain_userId,
CASE when t1.event_name ='page_view' and  t1.page_code='cart' THEN t1.domain_userId end cart_domain_userId,
CASE when t1.event_name ='page_view' and  t1.page_code='check_out' THEN t1.domain_userId end check_out_domain_userId,
CASE when t1.event_name ='page_view' and  t1.page_code='product_detail' THEN t1.domain_userId end product_detail_domain_userId,
CASE when t1.event_name ='data' and t1.page_code='product_detail' and t1.element_name = 'AddToCartSuccess' THEN t1.domain_userId end add_cart_success_domain_userId,
CASE when t1.event_name ='click' and t1.page_code='check_out' and t1.element_name = 'continue_checkout' THEN t1.domain_userId end continue_checkout_domain_userId
from
(
select pt,hour,datasource,domain_userId,platform,buyer_id,event_name,geo_country,os_type,page_code,device_id,referrer,view_type,NULL element_name,NULL list_name from dwd.dwd_vova_log_page_view_arc where pt>=date_sub('${cur_date}', 30) and pt<='${cur_date}' and datasource NOT IN ('vova')
union all
select pt,hour,datasource,domain_userId,platform,buyer_id,event_name,geo_country,os_type,page_code,device_id,referrer,NULL view_type,element_name,NULL list_name from dwd.dwd_vova_log_data_arc where pt>=date_sub('${cur_date}', 30) and pt<='${cur_date}' and datasource NOT IN ('vova')
union all
select pt,hour,datasource,domain_userId,platform,buyer_id,event_name,geo_country,os_type,page_code,device_id,referrer,NULL view_type,element_name,NULL list_name from dwd.dwd_vova_log_click_arc where pt>=date_sub('${cur_date}', 30) and pt<='${cur_date}' and datasource NOT IN ('vova') and event_type='normal'
) t1
LEFT JOIN dim.dim_zq_domain_userid fdu on fdu.domain_userid = t1.domain_userid AND fdu.datasource = t1.datasource
INNER JOIN dim.dim_zq_site zs on zs.datasource = t1.datasource AND zs.domain_group IN ('FD', 'TRIGRAM')
) log_data
group by cube(log_data.datasource, log_data.region_code, log_data.platform, log_data.user_source, log_data.pt, log_data.hour, nvl(log_data.is_new,'new'))
UNION ALL
select
nvl(nvl(oi.project_name,'NA'),'all') AS datasource,
nvl(date(oi.pay_time),'all') AS event_date,
nvl(if(HOUR(oi.pay_time)<10,concat('0', HOUR(oi.pay_time)),HOUR(oi.pay_time)),'all') AS hour,
nvl(nvl(r.region_code,'NA') ,'all') region_code,
nvl(if(oi.from_domain like '%api%', 'web', 'pc'),'all') platform,
nvl(nvl(fdu.original_channel, 'unknown'), 'all') AS user_source,
nvl(nvl(CASE WHEN datediff(date(oi.pay_time),fdu.activate_time)<=0 THEN 'new'
  WHEN datediff(date(oi.pay_time),fdu.activate_time)>=1 and datediff(date(oi.pay_time),fdu.activate_time)<6 THEN '2-7'
  WHEN datediff(date(oi.pay_time),fdu.activate_time)>=7 and datediff(date(oi.pay_time),fdu.activate_time)<29 THEN '8-30'
  WHEN fdu.activate_time IS NULL THEN 'new'
  else '30+' END, 'new'), 'all') AS is_new,
0 AS dau,
0 as home_page_uv,
0 as cart_uv,
0 as checkout_uv,
0 as product_detail_uv,
0 as checkout_click_success_uv,
0 as continue_checkout_uv,
sum(oi.goods_amount + oi.shipping_fee) AS gmv,
count(DISTINCT oi.user_id) AS paid_uv,
count(*) AS paid_order_cnt,
count(if(pre_order.min_order_id is not null, oi.order_id, null)) AS first_order_cnt
from
ods_zq_zsp.ods_zq_order_info_h oi
INNER JOIN dim.dim_zq_site zs on zs.datasource = oi.project_name AND zs.domain_group IN ('FD', 'TRIGRAM')
LEFT JOIN ods_zq_zsp.ods_zq_region r on r.region_id = oi.country
LEFT JOIN
(
select
datasource,
cur_buyer_id,
first(original_channel) as original_channel,
first(activate_time) as activate_time
from
dim.dim_zq_domain_userid fdu
group by cur_buyer_id, datasource
) fdu on fdu.cur_buyer_id = oi.user_id AND fdu.datasource = oi.project_name
LEFT JOIN (
SELECT oi.user_id,min(oi.order_id) AS min_order_id, oi.project_name
FROM ods_zq_zsp.ods_zq_order_info_h oi
WHERE oi.pay_status >= 1
group by oi.user_id, oi.project_name
) pre_order on pre_order.min_order_id = oi.order_id AND oi.project_name = pre_order.project_name
WHERE date(oi.pay_time) >= date_sub('${cur_date}', 30)
AND date(oi.pay_time) <= '${cur_date}'
AND oi.pay_status >= 1
GROUP BY CUBE (if(oi.from_domain like '%api%', 'web', 'pc'), date(oi.pay_time), nvl(r.region_code,'NA'), nvl(oi.project_name,'NA'),
nvl(CASE WHEN datediff(date(oi.pay_time),fdu.activate_time)<=0 THEN 'new'
  WHEN datediff(date(oi.pay_time),fdu.activate_time)>=1 and datediff(date(oi.pay_time),fdu.activate_time)<6 THEN '2-7'
  WHEN datediff(date(oi.pay_time),fdu.activate_time)>=7 and datediff(date(oi.pay_time),fdu.activate_time)<29 THEN '8-30'
  WHEN fdu.activate_time IS NULL THEN 'new'
  else '30+' END, 'new'),if(HOUR(oi.pay_time)<10,concat('0', HOUR(oi.pay_time)),HOUR(oi.pay_time)),
nvl(fdu.original_channel, 'unknown'))
) fin
group by fin.event_date, fin.region_code, fin.platform, fin.user_source, fin.is_new, fin.hour, fin.datasource
having event_date != 'all' AND hour = 'all'
;


INSERT OVERWRITE TABLE dwb.dwb_zq_main_goods PARTITION (domain_group='FN', pt)
SELECT
/*+ REPARTITION(1) */
    fin.pt AS event_date,
    fin.datasource,
    fin.platform,
    fin.region_code,
    fin.user_source,
    fin.is_new,
    nvl(fin.impressions, 0)    AS impressions,
    nvl(fin.impressions_uv, 0) AS impressions_uv,
    nvl(fin.clicks, 0)         AS clicks,
    nvl(fin.clicks_uv, 0)      AS clicks_uv,
    fin.hour,
    fin.pt
FROM (
         SELECT final.pt,
                final.hour,
                final.datasource,
                final.platform,
                final.region_code,
                final.user_source,
                final.is_new,
                sum(impressions) AS impressions,
                sum(impressions_uv) AS impressions_uv,
                sum(clicks)      AS clicks,
                sum(clicks_uv)      AS clicks_uv
         FROM (
                  SELECT nvl(log.pt, 'all') AS pt,
                         nvl(log.hour, 'all') AS hour,
                         nvl(log.datasource, 'all') AS datasource,
                         nvl(log.platform, 'all') AS platform,
                         nvl(nvl(log.geo_country,'NALL'),'all') region_code,
                         nvl(nvl(fdu.original_channel, 'unknown'),'all') user_source,
                         nvl(nvl(CASE WHEN datediff(log.pt,fdu.activate_time)<=0 THEN 'new'
                                      WHEN datediff(log.pt,fdu.activate_time)>=1 and datediff(log.pt,fdu.activate_time)<6 THEN '2-7'
                                      WHEN datediff(log.pt,fdu.activate_time)>=7 and datediff(log.pt,fdu.activate_time)<29 THEN '8-30'
                                      WHEN fdu.activate_time IS NULL THEN 'new'
                                      else '30+' END, 'new'), 'all') AS is_new,
                         count(*) AS impressions,
                         count(DISTINCT log.domain_userid) AS impressions_uv,
                         0        AS clicks,
                         0        AS clicks_uv
                  FROM dwd.dwd_vova_log_impressions_arc log
                   INNER JOIN dim.dim_zq_site zs on zs.datasource = log.datasource AND zs.domain_group = 'FN'
                   LEFT JOIN dim.dim_zq_domain_userid fdu on fdu.domain_userid = log.domain_userid AND fdu.datasource = log.datasource
                  WHERE log.pt >= date_sub('${cur_date}', 30) AND log.pt <= '${cur_date}'
                    AND log.platform IN ('pc', 'web')
                    AND log.event_type = 'goods'
                  GROUP BY CUBE (log.pt, log.hour, log.platform, nvl(log.geo_country,'NALL'), nvl(fdu.original_channel, 'unknown'),
                                 nvl(CASE WHEN datediff(log.pt,fdu.activate_time)<=0 THEN 'new'
                                   WHEN datediff(log.pt,fdu.activate_time)>=1 and datediff(log.pt,fdu.activate_time)<6 THEN '2-7'
                                   WHEN datediff(log.pt,fdu.activate_time)>=7 and datediff(log.pt,fdu.activate_time)<29 THEN '8-30'
                                   WHEN fdu.activate_time IS NULL THEN 'new'
                                   else '30+' END, 'new'), log.datasource
                                   )

                  UNION ALL

                  SELECT nvl(log.pt, 'all') AS pt,
                         nvl(log.hour, 'all') AS hour,
                         nvl(log.datasource, 'all') AS datasource,
                         nvl(log.platform, 'all') AS platform,
                         nvl(nvl(log.geo_country,'NALL'),'all') region_code,
                         nvl(nvl(fdu.original_channel, 'unknown'),'all') user_source,
                         nvl(nvl(CASE WHEN datediff(log.pt,fdu.activate_time)<=0 THEN 'new'
                                      WHEN datediff(log.pt,fdu.activate_time)>=1 and datediff(log.pt,fdu.activate_time)<6 THEN '2-7'
                                      WHEN datediff(log.pt,fdu.activate_time)>=7 and datediff(log.pt,fdu.activate_time)<29 THEN '8-30'
                                      WHEN fdu.activate_time IS NULL THEN 'new'
                                      else '30+' END, 'new'), 'all') AS is_new,
                         0        AS impressions,
                         0        AS impressions_uv,
                         count(*) AS clicks,
                         count(DISTINCT log.domain_userid) AS clicks_uv
                  FROM dwd.dwd_vova_log_click_arc log
                   INNER JOIN dim.dim_zq_site zs on zs.datasource = log.datasource AND zs.domain_group = 'FN'
                   LEFT JOIN dim.dim_zq_domain_userid fdu on fdu.domain_userid = log.domain_userid AND fdu.datasource = log.datasource
                  WHERE log.pt >= date_sub('${cur_date}', 30) AND log.pt <= '${cur_date}'
                    AND log.platform IN ('pc', 'web')
                    AND log.event_type = 'goods'
                  GROUP BY CUBE (log.pt, log.hour, log.platform, nvl(log.geo_country,'NALL'), nvl(fdu.original_channel, 'unknown'),
                                 nvl(CASE WHEN datediff(log.pt,fdu.activate_time)<=0 THEN 'new'
                                   WHEN datediff(log.pt,fdu.activate_time)>=1 and datediff(log.pt,fdu.activate_time)<6 THEN '2-7'
                                   WHEN datediff(log.pt,fdu.activate_time)>=7 and datediff(log.pt,fdu.activate_time)<29 THEN '8-30'
                                   WHEN fdu.activate_time IS NULL THEN 'new'
                                   else '30+' END, 'new'), log.datasource
                                   )
              ) final
         GROUP BY final.pt,final.hour, final.platform, final.region_code, final.user_source, final.is_new, final.datasource
         HAVING pt != 'all'
     ) fin
;
"

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=fn_main_process_h" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi


