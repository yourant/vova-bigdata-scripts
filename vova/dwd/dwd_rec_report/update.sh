#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
pre_week=`date -d "7 day ago ${cur_date}" +%Y-%m-%d`
pre_date=`date -d "1 day ago ${cur_date}" +%Y-%m-%d`
###逻辑sql
sql="
alter table dwd.dwd_vova_rec_report_clk_expre drop if exists partition(pt = '$pre_week');
alter table dwd.dwd_vova_rec_report_cart_cause drop if exists partition(pt = '$pre_week');
alter table dwd.dwd_vova_rec_report_order_cause drop if exists partition(pt = '$pre_week');
alter table dwd.dwd_vova_rec_report_pay_cause drop if exists partition(pt = '$pre_week');
insert overwrite table dwd.dwd_vova_rec_report_clk_expre PARTITION (pt = '$cur_date')
select
/*+ REPARTITION(1) */
nvl(gc.datasource,'NA') datasource,
nvl(geo_country,'NA') country,
nvl(os_type,'NA') os_type,
nvl(page_code,'NA') page_code,
nvl(element_type,'NA') element_type,
nvl(list_type,'NA') list_type,
case when page_code = 'homepage' and list_type='/popular' then 'rec_best_selling'
     when page_code in ('homepage','product_list') and list_type = '/product_list_newarrival' then 'rec_new_arrival'
     when page_code in ('homepage','product_list') and  list_type in ('/product_list_popular','/product_list') then 'rec_most_popular'
     when page_code in ('homepage','product_list') and  list_type = '/product_list_sold' then 'rec_sold'
     when page_code in ('homepage','product_list') and  list_type in ('/product_list_price_asc','/product_list_price_desc') then 'rec_price'
     when page_code ='flashsale' and list_type in ('/onsale','upcoming','/upcoming') then 'rec_flash_sale'
     when page_code ='product_detail' and list_type ='/detail_also_like' then 'rec_product_detail'
     when page_code ='search_result' and list_type in ('/search_result','/search_result_recommend') then 'rec_search_result'
     when page_code ='search_result' and list_type = '/search_result_sold' then 'rec_search_sold'
     when page_code ='search_result' and list_type in ('/search_result_price_desc','/search_result_price_asc') then 'rec_search_price'
     when page_code ='search_result' and list_type = '/search_result_newarrival' then 'rec_search_newarrival'
     when page_code ='coins_rewards' and list_type ='/coins_rewards' then 'rec_coins_rewards'
     when page_code ='cart' and list_type ='/cart_also_like' then 'rec_cart'
     when page_code ='merchant_store' and list_type in ('/merchant_store','merchant_store') then 'rec_merchant_store'
     when page_code ='me' and list_type ='/me_also_like' then 'rec_me'
     when page_code ='payment_success' and list_type ='/pay_success' then 'rec_payment_success'
     when page_code ='theme_activity' and list_type not like '%201912%' then 'rec_theme_activity'
     when page_code ='theme_activity' and list_type like '%201912%'  then 'rec_push'
	 when page_code = 'search_result' and list_type = '/search_result_also_like' then 'rec_search_noresult_recommend'
	 when (page_code = 'robert_guide_session' and list_type = '/robert_guide_session_list')
	   or (page_code = 'recommend_product_list' and list_type = '/robert_guide_also_like') then 'rec_robot'
     else 'others' end rec_page_code,
CASE WHEN datediff(gc.pt,d.activate_time)<=0 THEN 'new'
     WHEN datediff(gc.pt,d.activate_time)>=1 and datediff(gc.pt,d.activate_time)<2 THEN '2-3'
     WHEN datediff(gc.pt,d.activate_time)>=3 and datediff(gc.pt,d.activate_time)<6 THEN '4-7'
     WHEN datediff(gc.pt,d.activate_time)>=7 and datediff(gc.pt,d.activate_time)<29 THEN '8-30'
     else '30+' END activate_time,
gc.device_id device_id_clk,
null device_id_expre,
1 clks,
0 expres,
if(dg.brand_id >0,'Y','N') as is_brand,
if(get_rp_name(recall_pool) like '%47%','Y','N') brand_status
from dwd.dwd_vova_log_goods_click gc
LEFT JOIN dim.dim_vova_goods dg ON gc.virtual_goods_id = dg.virtual_goods_id
left join dim.dim_vova_devices d on d.device_id = gc.device_id and d.datasource=gc.datasource
where pt='$cur_date' and os_type in ('ios','android')
union all
select
/*+ REPARTITION(20) */
nvl(gi.datasource,'NA') datasource,
nvl(geo_country,'NA') country,
nvl(os_type,'NA') os_type,
nvl(page_code,'NA') page_code,
nvl(element_type,'NA') element_type,
nvl(list_type,'NA') list_type,
case when page_code = 'homepage' and list_type='/popular' then 'rec_best_selling'
     when page_code in ('homepage','product_list') and list_type = '/product_list_newarrival' then 'rec_new_arrival'
     when page_code in ('homepage','product_list') and  list_type in ('/product_list_popular','/product_list') then 'rec_most_popular'
     when page_code in ('homepage','product_list') and  list_type = '/product_list_sold' then 'rec_sold'
     when page_code in ('homepage','product_list') and  list_type in ('/product_list_price_asc','/product_list_price_desc') then 'rec_price'
     when page_code ='flashsale' and list_type in ('/onsale','upcoming','/upcoming') then 'rec_flash_sale'
     when page_code ='product_detail' and list_type ='/detail_also_like' then 'rec_product_detail'
     when page_code ='search_result' and list_type in ('/search_result','/search_result_recommend') then 'rec_search_result'
     when page_code ='search_result' and list_type = '/search_result_sold' then 'rec_search_sold'
     when page_code ='search_result' and list_type in ('/search_result_price_desc','/search_result_price_asc') then 'rec_search_price'
     when page_code ='search_result' and list_type = '/search_result_newarrival' then 'rec_search_newarrival'
     when page_code ='coins_rewards' and list_type ='/coins_rewards' then 'rec_coins_rewards'
     when page_code ='cart' and list_type ='/cart_also_like' then 'rec_cart'
     when page_code ='merchant_store' and list_type in ('/merchant_store','merchant_store') then 'rec_merchant_store'
     when page_code ='me' and list_type ='/me_also_like' then 'rec_me'
     when page_code ='payment_success' and list_type ='/pay_success' then 'rec_payment_success'
     when page_code ='theme_activity' and list_type not like '%201912%' then 'rec_theme_activity'
     when page_code ='theme_activity' and list_type like '%201912%'  then 'rec_push'
	 when page_code = 'search_result' and list_type = '/search_result_also_like' then 'rec_search_noresult_recommend'
	 when (page_code = 'robert_guide_session' and list_type = '/robert_guide_session_list')
	   or (page_code = 'recommend_product_list' and list_type = '/robert_guide_also_like') then 'rec_robot'
     else 'others' end rec_page_code,
CASE WHEN datediff(gi.pt,d.activate_time)<=0 THEN 'new'
     WHEN datediff(gi.pt,d.activate_time)>=1 and datediff(gi.pt,d.activate_time)<2 THEN '2-3'
     WHEN datediff(gi.pt,d.activate_time)>=3 and datediff(gi.pt,d.activate_time)<6 THEN '4-7'
     WHEN datediff(gi.pt,d.activate_time)>=7 and datediff(gi.pt,d.activate_time)<29 THEN '8-30'
     else '30+' END activate_time,
null device_id_clk,
gi.device_id device_id_expre,
0 clks,
1 expres,
if(dg.brand_id >0,'Y','N') as is_brand,
if(get_rp_name(recall_pool) like '%47%','Y','N') brand_status
from dwd.dwd_vova_log_goods_impression gi
LEFT JOIN dim.dim_vova_goods dg ON gi.virtual_goods_id = dg.virtual_goods_id
left join dim.dim_vova_devices d on d.device_id = gi.device_id and d.datasource=gi.datasource
where pt='$cur_date'  and os_type in ('ios','android');



insert overwrite table dwd.dwd_vova_rec_report_cart_cause  PARTITION (pt = '$cur_date')
select
/*+ REPARTITION(1) */
nvl(c.datasource,'NA') datasource,
nvl(c.platform,'NA') os_type,
nvl(country,'NA') country,
nvl(pre_page_code,'NA') page_code,
nvl(pre_element_type,'NA') element_type,
nvl(pre_list_type,'NA') list_type,
case when pre_page_code = 'homepage' and pre_list_type='/popular' then 'rec_best_selling'
     when pre_page_code in ('homepage','product_list') and pre_list_type = '/product_list_newarrival' then 'rec_new_arrival'
     when pre_page_code in ('homepage','product_list') and  pre_list_type in ('/product_list_popular','/product_list') then 'rec_most_popular'
     when pre_page_code in ('homepage','product_list') and  pre_list_type = '/product_list_sold' then 'rec_sold'
     when pre_page_code in ('homepage','product_list') and  pre_list_type in ('/product_list_price_asc','/product_list_price_desc') then 'rec_price'
     when pre_page_code ='flashsale' and pre_list_type in ('/onsale','upcoming','/upcoming') then 'rec_flash_sale'
     when pre_page_code ='product_detail' and pre_list_type ='/detail_also_like' then 'rec_product_detail'
     when pre_page_code ='search_result' and pre_list_type in ('/search_result','/search_result_recommend') then 'rec_search_result'
     when pre_page_code ='search_result' and pre_list_type = '/search_result_sold' then 'rec_search_sold'
     when pre_page_code ='search_result' and pre_list_type in ('/search_result_price_desc','/search_result_price_asc') then 'rec_search_price'
     when pre_page_code ='search_result' and pre_list_type = '/search_result_newarrival' then 'rec_search_newarrival'
     when pre_page_code ='coins_rewards' and pre_list_type ='/coins_rewards' then 'rec_coins_rewards'
     when pre_page_code ='cart' and pre_list_type ='/cart_also_like' then 'rec_cart'
     when pre_page_code ='merchant_store' and pre_list_type in ('/merchant_store','merchant_store') then 'rec_merchant_store'
     when pre_page_code ='me' and pre_list_type ='/me_also_like' then 'rec_me'
     when pre_page_code ='payment_success' and pre_list_type ='/pay_success' then 'rec_payment_success'
     when pre_page_code ='theme_activity' and pre_list_type not like '%201912%' then 'rec_theme_activity'
     when pre_page_code ='theme_activity' and pre_list_type like '%201912%'  then 'rec_push'
	 when pre_page_code = 'search_result' and pre_list_type = '/search_result_also_like' then 'rec_search_noresult_recommend'
	 when (pre_page_code = 'robert_guide_session' and pre_list_type = '/robert_guide_session_list')
	   or (pre_page_code = 'recommend_product_list' and pre_list_type = '/robert_guide_also_like') then 'rec_robot'
     else 'others' end rec_page_code,
CASE WHEN datediff(c.pt,d.activate_time)<=0 THEN 'new'
     WHEN datediff(c.pt,d.activate_time)>=1 and datediff(c.pt,d.activate_time)<2 THEN '2-3'
     WHEN datediff(c.pt,d.activate_time)>=3 and datediff(c.pt,d.activate_time)<6 THEN '4-7'
     WHEN datediff(c.pt,d.activate_time)>=7 and datediff(c.pt,d.activate_time)<29 THEN '8-30'
else '30+' END activate_time,
c.device_id,
if(dg.brand_id >0,'Y','N') as is_brand,
if(get_rp_name(pre_recall_pool) like '%47%','Y','N') brand_status
from dwd.dwd_vova_fact_cart_cause_v2 c
LEFT JOIN dim.dim_vova_goods dg ON c.virtual_goods_id = dg.virtual_goods_id
left join dim.dim_vova_devices d on d.device_id = c.device_id and d.datasource=c.datasource
where pt='$cur_date' and pre_page_code is not null;



insert overwrite table dwd.dwd_vova_rec_report_order_cause  PARTITION (pt = '$cur_date')
select
/*+ REPARTITION(1) */
nvl(og.datasource,'NA') datasource,
nvl(og.region_code,'NA') country,
nvl(og.platform,'NA') os_type,
nvl(pre_page_code,'NA') page_code,
nvl(pre_element_type,'NA') element_type,
nvl(pre_list_type,'NA') list_type,
case when pre_page_code = 'homepage' and pre_list_type='/popular' then 'rec_best_selling'
     when pre_page_code in ('homepage','product_list') and pre_list_type = '/product_list_newarrival' then 'rec_new_arrival'
     when pre_page_code in ('homepage','product_list') and  pre_list_type in ('/product_list_popular','/product_list') then 'rec_most_popular'
     when pre_page_code in ('homepage','product_list') and  pre_list_type = '/product_list_sold' then 'rec_sold'
     when pre_page_code in ('homepage','product_list') and  pre_list_type in ('/product_list_price_asc','/product_list_price_desc') then 'rec_price'
     when pre_page_code ='flashsale' and pre_list_type in ('/onsale','upcoming','/upcoming') then 'rec_flash_sale'
     when pre_page_code ='product_detail' and pre_list_type ='/detail_also_like' then 'rec_product_detail'
     when pre_page_code ='search_result' and pre_list_type in ('/search_result','/search_result_recommend') then 'rec_search_result'
     when pre_page_code ='search_result' and pre_list_type = '/search_result_sold' then 'rec_search_sold'
     when pre_page_code ='search_result' and pre_list_type in ('/search_result_price_desc','/search_result_price_asc') then 'rec_search_price'
     when pre_page_code ='search_result' and pre_list_type = '/search_result_newarrival' then 'rec_search_newarrival'
     when pre_page_code ='coins_rewards' and pre_list_type ='/coins_rewards' then 'rec_coins_rewards'
     when pre_page_code ='cart' and pre_list_type ='/cart_also_like' then 'rec_cart'
     when pre_page_code ='merchant_store' and pre_list_type in ('/merchant_store','merchant_store') then 'rec_merchant_store'
     when pre_page_code ='me' and pre_list_type ='/me_also_like' then 'rec_me'
     when pre_page_code ='payment_success' and pre_list_type ='/pay_success' then 'rec_payment_success'
     when pre_page_code ='theme_activity' and pre_list_type not like '%201912%' then 'rec_theme_activity'
     when pre_page_code ='theme_activity' and pre_list_type like '%201912%'  then 'rec_push'
	 when pre_page_code = 'search_result' and pre_list_type = '/search_result_also_like' then 'rec_search_noresult_recommend'
	 when (pre_page_code = 'robert_guide_session' and pre_list_type = '/robert_guide_session_list')
	   or (pre_page_code = 'recommend_product_list' and pre_list_type = '/robert_guide_also_like') then 'rec_robot'
     else 'others' end rec_page_code,
CASE WHEN datediff(date(og.order_time),d.activate_time)<=0 THEN 'new'
     WHEN datediff(date(og.order_time),d.activate_time)>=1 and datediff(date(og.order_time),d.activate_time)<2 THEN '2-3'
     WHEN datediff(date(og.order_time),d.activate_time)>=3 and datediff(date(og.order_time),d.activate_time)<6 THEN '4-7'
     WHEN datediff(date(og.order_time),d.activate_time)>=7 and datediff(date(og.order_time),d.activate_time)<29 THEN '8-30'
else '30+' END activate_time,
og.device_id,
og.buyer_id,
og.order_goods_id,
if(dg.brand_id >0,'Y','N') as is_brand,
if(get_rp_name(pre_recall_pool) like '%47%','Y','N') brand_status
from dim.dim_vova_order_goods og
LEFT JOIN dim.dim_vova_goods dg ON og.goods_id = dg.goods_id
left join dwd.dwd_vova_fact_order_cause_v2 oc on og.order_goods_id = oc.order_goods_id
left join dim.dim_vova_devices d on d.device_id = og.device_id and d.datasource=og.datasource
where date(og.order_time) ='$cur_date' and oc.pt='$cur_date'
and og.parent_rec_id =0
and (og.from_domain like '%api.vova%' or og.from_domain like '%api.airyclub%')
and oc.pre_page_code is not null;


insert overwrite table dwd.dwd_vova_rec_report_pay_cause  PARTITION (pt = '$cur_date')
select
/*+ REPARTITION(1) */
nvl(og.datasource,'NA') datasource,
nvl(og.region_code,'NA') country,
nvl(og.platform,'NA') os_type,
nvl(pre_page_code,'NA') page_code,
nvl(pre_element_type,'NA') element_type,
nvl(pre_list_type,'NA') list_type,
case when pre_page_code = 'homepage' and pre_list_type='/popular' then 'rec_best_selling'
     when pre_page_code in ('homepage','product_list') and pre_list_type = '/product_list_newarrival' then 'rec_new_arrival'
     when pre_page_code in ('homepage','product_list') and  pre_list_type in ('/product_list_popular','/product_list') then 'rec_most_popular'
     when pre_page_code in ('homepage','product_list') and  pre_list_type = '/product_list_sold' then 'rec_sold'
     when pre_page_code in ('homepage','product_list') and  pre_list_type in ('/product_list_price_asc','/product_list_price_desc') then 'rec_price'
     when pre_page_code ='flashsale' and pre_list_type in ('/onsale','upcoming','/upcoming') then 'rec_flash_sale'
     when pre_page_code ='product_detail' and pre_list_type ='/detail_also_like' then 'rec_product_detail'
     when pre_page_code ='search_result' and pre_list_type in ('/search_result','/search_result_recommend') then 'rec_search_result'
     when pre_page_code ='search_result' and pre_list_type = '/search_result_sold' then 'rec_search_sold'
     when pre_page_code ='search_result' and pre_list_type in ('/search_result_price_desc','/search_result_price_asc') then 'rec_search_price'
     when pre_page_code ='search_result' and pre_list_type = '/search_result_newarrival' then 'rec_search_newarrival'
     when pre_page_code ='coins_rewards' and pre_list_type ='/coins_rewards' then 'rec_coins_rewards'
     when pre_page_code ='cart' and pre_list_type ='/cart_also_like' then 'rec_cart'
     when pre_page_code ='merchant_store' and pre_list_type in ('/merchant_store','merchant_store') then 'rec_merchant_store'
     when pre_page_code ='me' and pre_list_type ='/me_also_like' then 'rec_me'
     when pre_page_code ='payment_success' and pre_list_type ='/pay_success' then 'rec_payment_success'
     when pre_page_code ='theme_activity' and pre_list_type not like '%201912%' then 'rec_theme_activity'
     when pre_page_code ='theme_activity' and pre_list_type like '%201912%'  then 'rec_push'
	 when pre_page_code = 'search_result' and pre_list_type = '/search_result_also_like' then 'rec_search_noresult_recommend'
	 when (pre_page_code = 'robert_guide_session' and pre_list_type = '/robert_guide_session_list')
	   or (pre_page_code = 'recommend_product_list' and pre_list_type = '/robert_guide_also_like') then 'rec_robot'
     else 'others' end rec_page_code,
CASE WHEN datediff(date(og.pay_time),d.activate_time)<=0 THEN 'new'
     WHEN datediff(date(og.pay_time),d.activate_time)>=1 and datediff(date(og.pay_time),d.activate_time)<2 THEN '2-3'
     WHEN datediff(date(og.pay_time),d.activate_time)>=3 and datediff(date(og.pay_time),d.activate_time)<6 THEN '4-7'
     WHEN datediff(date(og.pay_time),d.activate_time)>=7 and datediff(date(og.pay_time),d.activate_time)<29 THEN '8-30'
else '30+' END activate_time,
og.device_id,
og.buyer_id,
og.order_goods_id,og.goods_number,
og.goods_number * og.shop_price + og.shipping_fee as gmv,
if(dg.brand_id >0,'Y','N') as is_brand,
if(get_rp_name(pre_recall_pool) like '%47%','Y','N') brand_status
from dwd.dwd_vova_fact_pay og
LEFT JOIN dim.dim_vova_goods dg ON og.goods_id = dg.goods_id
left join dwd.dwd_vova_fact_order_cause_v2 oc on og.order_goods_id = oc.order_goods_id
left join dim.dim_vova_devices d on d.device_id = og.device_id and d.datasource=og.datasource
where date(og.pay_time) ='$cur_date' and (oc.pt>='$pre_week' and oc.pt<='$cur_date')
and (og.from_domain like '%api.vova%' or og.from_domain like '%api.airyclub%')
and oc.pre_page_code is not null;

"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql  --conf "spark.sql.parquet.writeLegacyFormat=true" --conf "spark.sql.crossJoin.enabled=true" --conf "spark.sql.adaptive.shuffle.targetPostShuffleInputSize=128000000" --conf "spark.sql.adaptive.enabled=true" --conf "spark.app.name=dwd_vova_rec_report" -e "$sql"
#hive -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi