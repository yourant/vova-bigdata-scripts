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
-- ctr相关
drop table if exists tmp.vova_rec_report_clk_expre_h;
create table tmp.vova_rec_report_clk_expre_h as
select
nvl(hour,25) hour,
nvl(datasource,'all') datasource,
nvl(country,'all') country,
nvl(os_type,'all') os_type,
nvl(rec_page_code,'all') rec_page_code,
sum(clicks) clk,
sum(impressions) expre,
sum(clicks)/sum(impressions) ctr,
count(distinct click_device_id) clk_uv,
count(distinct impression_device_id) expre_uv
from
(
select hour(collector_ts) hour,datasource,os_type, device_id click_device_id,null impression_device_id,1 clicks,0 impressions,
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
     else 'others' end rec_page_code,
nvl(geo_country,'NALL') country
from dwd.dwd_vova_log_goods_click_arc where pt='$pt' and  collector_ts <'$etime'  and  os_type in ('ios','android')
union all
select hour(collector_ts) hour,datasource, os_type,device_id click_device_id,null impression_device_id,1 clicks,0 impressions,
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
     else 'others' end rec_page_code,
nvl(geo_country,'NALL') country
from dwd.dwd_vova_log_click_arc where pt='$pt' and collector_ts < '$etime' and  os_type in ('ios','android') and event_type='goods'
union all
select hour(collector_ts) hour,datasource, os_type,null click_device_id,device_id impression_device_id,0 clicks,1 impressions,
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
     else 'others' end rec_page_code,
nvl(geo_country,'NALL') country
from dwd.dwd_vova_log_goods_impression_arc where pt='$pt' and  collector_ts <'$etime'  and  os_type in ('ios','android')
union all
select hour(collector_ts) hour,datasource, os_type,null click_device_id,device_id impression_device_id,0 clicks,1 impressions,
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
     else 'others' end rec_page_code,
nvl(geo_country,'NALL') country
from dwd.dwd_vova_log_impressions_arc where pt='$pt' and collector_ts < '$etime' and  os_type in ('ios','android') and event_type='goods'
) group by hour,datasource,country,os_type,rec_page_code with cube;

--加车相关
drop table if exists tmp.vova_rec_report_cart_cause_h;
create table tmp.vova_rec_report_cart_cause_h as
select
nvl(hour,25) hour,
nvl(datasource,'all') datasource,
nvl(country,'all') country,
nvl(os_type,'all') os_type,
nvl(rec_page_code,'all') rec_page_code,
count(distinct device_id) cart_uv
from
(
select
hour(from_unixtime(collector_tstamp / 1000,'yyyy-MM-dd HH:mm:ss')) hour,
nvl(datasource,'NA') datasource,
nvl(platform,'NA') os_type,
nvl(country,'NALL') country,
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
     else 'others' end rec_page_code,
device_id
from dwd.dwd_vova_fact_cart_cause_h where pt='$pt' and collector_tstamp < '$etime' and  platform in ('ios','android') and pre_page_code is not null
) group by hour,datasource,country,os_type,rec_page_code with cube;

--下单相关
drop table if exists tmp.vova_rec_report_order_cause_h;
create table tmp.vova_rec_report_order_cause_h as
select
nvl(hour,25) hour,
nvl(datasource,'all') datasource,
nvl(country,'all') country,
nvl(os_type,'all') os_type,
nvl(rec_page_code,'all') rec_page_code,
count(distinct order_goods_id) ord_cnt
from
(
select
hour(oi.order_time) hour,
nvl(oc.datasource,'NA') datasource,
nvl(r.region_code,'NA') country,
nvl(oc.platform,'NA') os_type,
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
     else 'others' end rec_page_code,
oc.device_id,
oc.buyer_id,
oc.order_goods_id
from ods_vova_vts.ods_vova_order_goods_h og
join ods_vova_vts.ods_vova_order_info_h oi on og.order_id = oi.order_id
join dwd.dwd_vova_fact_order_cause_h oc on og.rec_id = oc.order_goods_id
join ods_vova_vts.ods_vova_region_h r on oi.country = r.region_id
where date(oi.order_time) ='$pt' and oi.order_time < '$etime' and oc.pt='$pt'
and oi.parent_order_id =0
and (oi.from_domain like '%api.vova%' or oi.from_domain like '%api.airyclub%')
and oc.pre_page_code is not null
) group by hour,datasource,country,os_type,rec_page_code with cube;

--支付相关
drop table if exists tmp.vova_rec_report_pay_cause_h;
create table tmp.vova_rec_report_pay_cause_h as
select
nvl(hour,25) hour,
nvl(datasource,'all') datasource,
nvl(country,'all') country,
nvl(os_type,'all') os_type,
nvl(rec_page_code,'all') rec_page_code,
count(distinct order_goods_id)  as pay_ord_cnt,
count(distinct buyer_id) as pay_uv,
sum(gmv) as gmv
from
(
select
hour(oi.pay_time) hour,
nvl(oc.datasource,'NA') datasource,
nvl(r.region_code,'NA') country,
nvl(oc.platform,'NA') os_type,
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
     else 'others' end rec_page_code,
oc.device_id,
oc.buyer_id,
oc.order_goods_id,
og.goods_number * og.shop_price + og.shipping_fee as gmv
from ods_vova_vts.ods_vova_order_goods_h og
join ods_vova_vts.ods_vova_order_info_h oi on og.order_id = oi.order_id
join dwd.dwd_vova_fact_order_cause_h oc on og.rec_id = oc.order_goods_id
join ods_vova_vts.ods_vova_region_h r on oi.country = r.region_id
where date(oi.pay_time) ='$pt' and oi.pay_time < '$etime'  and (oc.pt>='$pre_week' and oc.pt<='$pt')
and (oi.from_domain like '%api.vova%' or oi.from_domain like '%api.airyclub%')
and oi.parent_order_id =0 and oi.pay_status >=1
and oc.pre_page_code is not null
) group by hour,datasource,country,os_type,rec_page_code with cube;

insert overwrite table dwb.dwb_vova_rec_report_base_h  PARTITION (pt = '$pt')
select
/*+ REPARTITION(1) */
ce.hour,
ce.datasource,
ce.country,
ce.os_type,
ce.rec_page_code,
nvl(ce.clk,0) clk,
nvl(ce.expre,0) expre,
nvl(ce.clk/ce.expre,0) ctr,
nvl(ce.clk_uv,0) clk_uv,
nvl(ce.expre_uv,0) expre_uv,
nvl(c.cart_uv,0) cart_uv,
nvl(c.cart_uv/ce.expre_uv,0) cart_uv_expre_uv,
nvl(oc.ord_cnt,0) ord_cnt,
nvl(pc.pay_ord_cnt,0) pay_ord_cnt,
nvl(pc.pay_uv,0) pay_uv,
nvl(pc.pay_uv/ce.expre_uv,0) pay_uv_expre_uv,
nvl(pc.gmv,0) gmv
from
tmp.vova_rec_report_clk_expre_h ce
left join tmp.vova_rec_report_cart_cause_h c on ce.hour = c.hour and ce.datasource = c.datasource and ce.os_type = c.os_type and ce.rec_page_code = c.rec_page_code and ce.country = c.country
left join tmp.vova_rec_report_order_cause_h oc on ce.hour = oc.hour and ce.datasource = oc.datasource and ce.os_type = oc.os_type and ce.rec_page_code = oc.rec_page_code and ce.country = oc.country
left join tmp.vova_rec_report_pay_cause_h pc on ce.hour = pc.hour and ce.datasource = pc.datasource and ce.os_type = pc.os_type and ce.rec_page_code = pc.rec_page_code and ce.country = pc.country;

-- 同比计算
drop table if exists tmp.rec_report_yoy_h;
create table tmp.rec_report_yoy_h as
select
/*+ REPARTITION(1) */
t1.hour,
t1.datasource,
t1.country,
t1.os_type,
t1.rec_page_code,
nvl(t1.pay_uv_expre_uv- t2.pay_uv_expre_uv,0) pay_uv_expre_uv_yoy
from
(select hour,datasource,os_type,country,rec_page_code,pay_uv_expre_uv from dwb.dwb_vova_rec_report_base_h where pt ='$pt') t1
left join
(select hour,datasource,os_type,country,rec_page_code,pay_uv_expre_uv from dwb.dwb_vova_rec_report_base_h where pt ='$pre_pt') t2
on t1.hour = t2.hour and t1.datasource =t2.datasource and t1.os_type=t2.os_type and t1.country=t2.country and t1.rec_page_code=t2.rec_page_code;

insert overwrite table dwb.dwb_vova_rec_report_h  PARTITION (pt = '$pt')
select
case when b.hour != 25 then from_unixtime(unix_timestamp(to_date('$pt'))+b.hour*60*60,'yyyy-MM-dd HH:00:00')
else  from_unixtime(unix_timestamp(to_date('$pt'))+23*60*60,'yyyy-MM-dd 23:59:59') end event_date,
b.hour,
b.datasource,
b.country,
b.os_type,
b.rec_page_code,
b.clk,
b.expre,
b.ctr,
b.clk_uv,
b.expre_uv,
b.cart_uv,
b.cart_uv_expre_uv,
b.ord_cnt,
b.pay_ord_cnt,
b.pay_uv,
b.pay_uv_expre_uv,
b.gmv,
y.pay_uv_expre_uv_yoy
from
dwb.dwb_vova_rec_report_base_h b
left join tmp.rec_report_yoy_h y on b.hour = y.hour and b.datasource =y.datasource and b.os_type=y.os_type and b.country=y.country and b.rec_page_code=y.rec_page_code
where b.pt='$pt';
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=dwb_vova_rec_report_h" \
--conf "spark.default.parallelism = 430" \
--conf "spark.sql.shuffle.partitions=430" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
--conf "spark.sql.crossJoin.enabled=true" \
-e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
