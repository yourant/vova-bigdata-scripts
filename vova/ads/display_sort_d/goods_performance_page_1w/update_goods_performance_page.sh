#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
#dependence
#dwd_vova_log_goods_impression
#dwd_vova_log_goods_click
#dwd_vova_log_common_click
#dwd_vova_fact_pay
#dim_vova_goods
#dim_vova_merchant
#dim_vova_category
#ods_vova_brand
sql="
drop table if exists tmp.tmp_ads_vova_goods_performance_page_group;
create table tmp.tmp_ads_vova_goods_performance_page_group as
select
/*+ REPARTITION(10) */
'app_group' AS datasource,
t1.rec_page_code,
t1.platform,
t1.region_code,
t1.goods_id,
dg.goods_sn,
t1.sales_order,
t1.gmv,
t1.impressions,
t1.clicks,
t1.users,
t1.ctr,
t1.rate,
t1.gr,
t1.gcr,
t1.last_update_time,
nvl(dg.first_cat_name, '') as first_cat_name,
nvl(dg.second_cat_name, '') as second_cat_name,
nvl(dg.first_cat_id, 0) as first_cat_id,
nvl(dg.second_cat_id, 0) as second_cat_id,
nvl(dg.shop_price + dg.shipping_fee, 0) as shop_price_amount,
nvl(dg.is_on_sale, 0) as is_on_sale,
nvl(dg.brand_id, 0) as brand_id,
nvl(b.brand_name, '') as brand_name,
nvl(dm.mct_name, '') as mct_name,
nvl(dg.mct_id, 0) as mct_id,
nvl(c.three_cat_id, '/') as third_cat_id,
nvl(c.three_cat_name, '/') as third_cat_name,
nvl(c.four_cat_id, '/') as fourth_cat_id,
nvl(c.four_cat_name, '/') as fourth_cat_name
from
(
select
datasource,
rec_page_code,
'mob' AS platform,
region_code,
goods_id,
SUM(sales_order) AS sales_order,
SUM(gmv) AS gmv,
SUM(impressions) AS impressions,
SUM(clicks) AS clicks,
SUM(users) AS users,
nvl(SUM(clicks) / SUM(impressions), 0) as ctr,
nvl(SUM(sales_order) / SUM(users), 0) as rate,
nvl(SUM(gmv) / SUM(users) * 100, 0) as gr,
nvl(SUM(gmv) / SUM(users) * SUM(clicks) / SUM(impressions) * 10000, 0) as gcr,
current_timestamp() AS last_update_time
from
(
select
nvl(t1.datasource, 'all') AS datasource,
nvl(t1.region_code, 'all') AS region_code,
nvl(t1.rec_page_code, 'all') AS rec_page_code,
nvl(t1.goods_id, 'all') AS goods_id,
COUNT(DISTINCT order_id) AS sales_order,
SUM(gmv) AS gmv,
0 AS impressions,
0 AS clicks,
0 AS users
from
(
select
og.datasource,
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
og.goods_id,
og.buyer_id,
og.order_id,
og.region_code,
og.goods_number * og.shop_price + og.shipping_fee as gmv
from dwd.dwd_vova_fact_pay og
left join dwd.dwd_vova_fact_order_cause_v2 oc on og.order_goods_id = oc.order_goods_id
where date(og.pay_time) >= date_sub('${cur_date}', 6)
and date(og.pay_time) <=  '${cur_date}'
and (og.from_domain like '%api%')
AND og.datasource not in ('vova', 'ac')
and oc.pre_page_code is not null
) t1
group by cube (t1.datasource, t1.rec_page_code, t1.goods_id, t1.region_code)

UNION ALL

SELECT
nvl(datasource, 'all') AS datasource,
nvl(nvl(geo_country, 'NA'), 'all') AS region_code,
nvl(rec_page_code, 'all') AS rec_page_code,
nvl(goods_id, 'all') AS goods_id,
0 AS sales_order,
0 AS gmv,
count(*) AS impressions,
0 AS clicks,
0 AS users
FROM (
SELECT
log.datasource,
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
dg.goods_id,
log.geo_country,
log.device_id
FROM dwd.dwd_vova_log_goods_impression log
inner join dim.dim_vova_goods dg on dg.virtual_goods_id = log.virtual_goods_id
INNER JOIN ods_vova_vtsf.ods_vova_acg_app a on a.data_domain = log.datasource
WHERE log.pt >= date_sub('${cur_date}', 6)
  AND log.pt <= '${cur_date}'
  AND log.platform = 'mob'
  
  AND log.dp = 'others'
  AND log.datasource not in ('vova', 'ac')
     ) temp
group by cube (datasource, rec_page_code, goods_id, nvl(geo_country, 'NA'))

UNION ALL

SELECT
nvl(datasource, 'all') AS datasource,
nvl(nvl(geo_country, 'NA'), 'all') AS region_code,
nvl(rec_page_code, 'all') AS rec_page_code,
nvl(goods_id, 'all') AS goods_id,
0 AS sales_order,
0 AS gmv,
0 AS impressions,
count(*) AS clicks,
count(distinct device_id) AS users
FROM (
SELECT
log.datasource,
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
dg.goods_id,
log.geo_country,
log.device_id
FROM dwd.dwd_vova_log_goods_click log
inner join dim.dim_vova_goods dg on dg.virtual_goods_id = log.virtual_goods_id
INNER JOIN ods_vova_vtsf.ods_vova_acg_app a on a.data_domain = log.datasource
WHERE log.pt >= date_sub('${cur_date}', 6)
  AND log.pt <= '${cur_date}'
  AND log.platform = 'mob'
  
  AND log.dp = 'others'
  AND log.datasource not in ('vova', 'ac')
     ) temp
group by cube (datasource, rec_page_code, goods_id, nvl(geo_country, 'NA'))

UNION ALL

select
nvl(t1.datasource, 'all') AS datasource,
nvl(nvl(t1.region_code, 'NALL'), 'all') AS region_code,
'search_list_product_detail' AS rec_page_code,
nvl(t1.goods_id, 'all') AS goods_id,
COUNT(DISTINCT order_id) AS sales_order,
SUM(gmv) AS gmv,
0 AS impressions,
0 AS clicks,
0 AS users
from
(
select
og.datasource,
og.region_code,
case when pre_page_code in ('homepage','product_list') and pre_list_type = '/product_list_newarrival' then 'rec_new_arrival'
     when pre_page_code in ('homepage','product_list') and  pre_list_type in ('/product_list_popular','/product_list') then 'rec_most_popular'
     when pre_page_code in ('homepage','product_list') and  pre_list_type = '/product_list_sold' then 'rec_sold'
     when pre_page_code in ('homepage','product_list') and  pre_list_type in ('/product_list_price_asc','/product_list_price_desc') then 'rec_price'
     when pre_page_code ='product_detail' and pre_list_type ='/detail_also_like' then 'rec_product_detail'
     when pre_page_code ='search_result' and pre_list_type in ('/search_result','/search_result_recommend') then 'rec_search_result'
     when pre_page_code ='search_result' and pre_list_type = '/search_result_sold' then 'rec_search_sold'
     when pre_page_code ='search_result' and pre_list_type in ('/search_result_price_desc','/search_result_price_asc') then 'rec_search_price'
     when pre_page_code ='search_result' and pre_list_type = '/search_result_newarrival' then 'rec_search_newarrival'
     else 'others' end rec_page_code,
og.goods_id,
og.buyer_id,
og.order_id,
og.goods_number * og.shop_price + og.shipping_fee as gmv
from dwd.dwd_vova_fact_pay og
left join dwd.dwd_vova_fact_order_cause_v2 oc on og.order_goods_id = oc.order_goods_id
where date(og.pay_time) >= date_sub('${cur_date}', 6)
and date(og.pay_time) <=  '${cur_date}'
and (og.from_domain like '%api%')
and oc.pre_page_code is not null
AND og.datasource not in ('vova', 'ac')
) t1
WHERE rec_page_code != 'others'
group by cube (t1.datasource, t1.goods_id, nvl(t1.region_code, 'NALL'))

UNION ALL

SELECT
nvl(datasource, 'all') AS datasource,
nvl(nvl(temp.region_code, 'NALL'), 'all') AS region_code,
'search_list_product_detail' AS rec_page_code,
nvl(goods_id, 'all') AS goods_id,
0 AS sales_order,
0 AS gmv,
count(*) AS impressions,
0 AS clicks,
0 AS users
FROM (
SELECT
log.datasource,
log.geo_country AS region_code,
case when page_code in ('homepage','product_list') and list_type = '/product_list_newarrival' then 'rec_new_arrival'
     when page_code in ('homepage','product_list') and  list_type in ('/product_list_popular','/product_list') then 'rec_most_popular'
     when page_code in ('homepage','product_list') and  list_type = '/product_list_sold' then 'rec_sold'
     when page_code in ('homepage','product_list') and  list_type in ('/product_list_price_asc','/product_list_price_desc') then 'rec_price'
     when page_code ='product_detail' and list_type ='/detail_also_like' then 'rec_product_detail'
     when page_code ='search_result' and list_type in ('/search_result','/search_result_recommend') then 'rec_search_result'
     when page_code ='search_result' and list_type = '/search_result_sold' then 'rec_search_sold'
     when page_code ='search_result' and list_type in ('/search_result_price_desc','/search_result_price_asc') then 'rec_search_price'
     when page_code ='search_result' and list_type = '/search_result_newarrival' then 'rec_search_newarrival'
     else 'others' end rec_page_code,
dg.goods_id,
log.device_id
FROM dwd.dwd_vova_log_goods_impression log
inner join dim.dim_vova_goods dg on dg.virtual_goods_id = log.virtual_goods_id
INNER JOIN ods_vova_vtsf.ods_vova_acg_app a on a.data_domain = log.datasource
WHERE log.pt >= date_sub('${cur_date}', 6)
  AND log.pt <= '${cur_date}'
  AND log.platform = 'mob'
  
  AND log.dp = 'others'
  AND log.datasource not in ('vova', 'ac')
     ) temp
WHERE rec_page_code != 'others'
group by cube (datasource, goods_id, nvl(temp.region_code, 'NALL'))

UNION ALL

SELECT
nvl(datasource, 'all') AS datasource,
nvl(nvl(temp.region_code, 'NALL'), 'all') AS region_code,
'search_list_product_detail' AS rec_page_code,
nvl(goods_id, 'all') AS goods_id,
0 AS sales_order,
0 AS gmv,
0 AS impressions,
count(*) AS clicks,
count(distinct device_id) AS users
FROM (
SELECT
log.datasource,
log.geo_country AS region_code,
case when page_code in ('homepage','product_list') and list_type = '/product_list_newarrival' then 'rec_new_arrival'
     when page_code in ('homepage','product_list') and  list_type in ('/product_list_popular','/product_list') then 'rec_most_popular'
     when page_code in ('homepage','product_list') and  list_type = '/product_list_sold' then 'rec_sold'
     when page_code in ('homepage','product_list') and  list_type in ('/product_list_price_asc','/product_list_price_desc') then 'rec_price'
     when page_code ='product_detail' and list_type ='/detail_also_like' then 'rec_product_detail'
     when page_code ='search_result' and list_type in ('/search_result','/search_result_recommend') then 'rec_search_result'
     when page_code ='search_result' and list_type = '/search_result_sold' then 'rec_search_sold'
     when page_code ='search_result' and list_type in ('/search_result_price_desc','/search_result_price_asc') then 'rec_search_price'
     when page_code ='search_result' and list_type = '/search_result_newarrival' then 'rec_search_newarrival'
     else 'others' end rec_page_code,
dg.goods_id,
log.device_id
FROM dwd.dwd_vova_log_goods_click log
inner join dim.dim_vova_goods dg on dg.virtual_goods_id = log.virtual_goods_id
INNER JOIN ods_vova_vtsf.ods_vova_acg_app a on a.data_domain = log.datasource
WHERE log.pt >= date_sub('${cur_date}', 6)
  AND log.pt <= '${cur_date}'
  AND log.platform = 'mob'
  
  AND log.dp = 'others'
  AND log.datasource not in ('vova', 'ac')
     ) temp
WHERE rec_page_code != 'others'
group by cube (datasource, goods_id, nvl(temp.region_code, 'NALL'))
)
GROUP BY datasource, rec_page_code, goods_id, region_code
) t1
inner join dim.dim_vova_goods dg on dg.goods_id = t1.goods_id
INNER JOIN dim.dim_vova_merchant dm ON dm.mct_id = dg.mct_id
LEFT JOIN ods_vova_vts.ods_vova_brand b ON b.brand_id = dg.brand_id
left join dim.dim_vova_category c on dg.cat_id = c.cat_id
WHERE (t1.clicks > 0 OR t1.sales_order > 0)
AND t1.goods_id != 'all' AND t1.region_code IN ('all', 'FR', 'DE', 'IT', 'ES', 'GB', 'TW')
AND t1.datasource = 'all'
;


INSERT OVERWRITE TABLE ads.ads_vova_goods_performance_page PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(10) */
t1.datasource,
t1.rec_page_code,
t1.platform,
t1.region_code,
t1.goods_id,
dg.goods_sn,
t1.sales_order,
t1.gmv,
t1.impressions,
t1.clicks,
t1.users,
t1.ctr,
t1.rate,
t1.gr,
t1.gcr,
t1.last_update_time,
nvl(dg.first_cat_name, '') as first_cat_name,
nvl(dg.second_cat_name, '') as second_cat_name,
nvl(dg.first_cat_id, 0) as first_cat_id,
nvl(dg.second_cat_id, 0) as second_cat_id,
nvl(dg.shop_price + dg.shipping_fee, 0) as shop_price_amount,
nvl(dg.is_on_sale, 0) as is_on_sale,
nvl(dg.brand_id, 0) as brand_id,
nvl(b.brand_name, '') as brand_name,
nvl(dm.mct_name, '') as mct_name,
nvl(dg.mct_id, 0) as mct_id,
nvl(c.three_cat_id, '/') as third_cat_id,
nvl(c.three_cat_name, '/') as third_cat_name,
nvl(c.four_cat_id, '/') as fourth_cat_id,
nvl(c.four_cat_name, '/') as fourth_cat_name
from
(
select
datasource,
rec_page_code,
'mob' AS platform,
region_code,
goods_id,
SUM(sales_order) AS sales_order,
SUM(gmv) AS gmv,
SUM(impressions) AS impressions,
SUM(clicks) AS clicks,
SUM(users) AS users,
nvl(SUM(clicks) / SUM(impressions), 0) as ctr,
nvl(SUM(sales_order) / SUM(users), 0) as rate,
nvl(SUM(gmv) / SUM(users) * 100, 0) as gr,
nvl(SUM(gmv) / SUM(users) * SUM(clicks) / SUM(impressions) * 10000, 0) as gcr,
current_timestamp() AS last_update_time
from
(
select
nvl(t1.datasource, 'all') AS datasource,
nvl(t1.region_code, 'all') AS region_code,
nvl(t1.rec_page_code, 'all') AS rec_page_code,
nvl(t1.goods_id, 'all') AS goods_id,
COUNT(DISTINCT order_id) AS sales_order,
SUM(gmv) AS gmv,
0 AS impressions,
0 AS clicks,
0 AS users
from
(
select
og.datasource,
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
og.goods_id,
og.buyer_id,
og.order_id,
og.region_code,
og.goods_number * og.shop_price + og.shipping_fee as gmv
from dwd.dwd_vova_fact_pay og
left join dwd.dwd_vova_fact_order_cause_v2 oc on og.order_goods_id = oc.order_goods_id
where date(og.pay_time) >= date_sub('${cur_date}', 6)
and date(og.pay_time) <=  '${cur_date}'
and (og.from_domain like '%api%')
and oc.pre_page_code is not null
) t1
group by cube (t1.datasource, t1.rec_page_code, t1.goods_id, t1.region_code)

UNION ALL

SELECT
nvl(datasource, 'all') AS datasource,
nvl(nvl(geo_country, 'NA'), 'all') AS region_code,
nvl(rec_page_code, 'all') AS rec_page_code,
nvl(goods_id, 'all') AS goods_id,
0 AS sales_order,
0 AS gmv,
count(*) AS impressions,
0 AS clicks,
0 AS users
FROM (
SELECT
log.datasource,
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
dg.goods_id,
log.geo_country,
log.device_id
FROM dwd.dwd_vova_log_goods_impression log
inner join dim.dim_vova_goods dg on dg.virtual_goods_id = log.virtual_goods_id
INNER JOIN ods_vova_vtsf.ods_vova_acg_app a on a.data_domain = log.datasource
WHERE log.pt >= date_sub('${cur_date}', 6)
  AND log.pt <= '${cur_date}'
  AND log.platform = 'mob'
  
     ) temp
group by cube (datasource, rec_page_code, goods_id, nvl(geo_country, 'NA'))

UNION ALL

SELECT
nvl(datasource, 'all') AS datasource,
nvl(nvl(geo_country, 'NA'), 'all') AS region_code,
nvl(rec_page_code, 'all') AS rec_page_code,
nvl(goods_id, 'all') AS goods_id,
0 AS sales_order,
0 AS gmv,
0 AS impressions,
count(*) AS clicks,
count(distinct device_id) AS users
FROM (
SELECT
log.datasource,
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
dg.goods_id,
log.geo_country,
log.device_id
FROM dwd.dwd_vova_log_goods_click log
inner join dim.dim_vova_goods dg on dg.virtual_goods_id = log.virtual_goods_id
INNER JOIN ods_vova_vtsf.ods_vova_acg_app a on a.data_domain = log.datasource
WHERE log.pt >= date_sub('${cur_date}', 6)
  AND log.pt <= '${cur_date}'
  AND log.platform = 'mob'
  
     ) temp
group by cube (datasource, rec_page_code, goods_id, nvl(geo_country, 'NA'))

UNION ALL

select
nvl(t1.datasource, 'all') AS datasource,
nvl(nvl(t1.region_code, 'NALL'), 'all') AS region_code,
'search_list_product_detail' AS rec_page_code,
nvl(t1.goods_id, 'all') AS goods_id,
COUNT(DISTINCT order_id) AS sales_order,
SUM(gmv) AS gmv,
0 AS impressions,
0 AS clicks,
0 AS users
from
(
select
og.datasource,
og.region_code,
case when pre_page_code in ('homepage','product_list') and pre_list_type = '/product_list_newarrival' then 'rec_new_arrival'
     when pre_page_code in ('homepage','product_list') and  pre_list_type in ('/product_list_popular','/product_list') then 'rec_most_popular'
     when pre_page_code in ('homepage','product_list') and  pre_list_type = '/product_list_sold' then 'rec_sold'
     when pre_page_code in ('homepage','product_list') and  pre_list_type in ('/product_list_price_asc','/product_list_price_desc') then 'rec_price'
     when pre_page_code ='product_detail' and pre_list_type ='/detail_also_like' then 'rec_product_detail'
     when pre_page_code ='search_result' and pre_list_type in ('/search_result','/search_result_recommend') then 'rec_search_result'
     when pre_page_code ='search_result' and pre_list_type = '/search_result_sold' then 'rec_search_sold'
     when pre_page_code ='search_result' and pre_list_type in ('/search_result_price_desc','/search_result_price_asc') then 'rec_search_price'
     when pre_page_code ='search_result' and pre_list_type = '/search_result_newarrival' then 'rec_search_newarrival'
     else 'others' end rec_page_code,
og.goods_id,
og.buyer_id,
og.order_id,
og.goods_number * og.shop_price + og.shipping_fee as gmv
from dwd.dwd_vova_fact_pay og
left join dwd.dwd_vova_fact_order_cause_v2 oc on og.order_goods_id = oc.order_goods_id
where date(og.pay_time) >= date_sub('${cur_date}', 6)
and date(og.pay_time) <=  '${cur_date}'
and (og.from_domain like '%api%')
and oc.pre_page_code is not null
) t1
WHERE rec_page_code != 'others'
group by cube (t1.datasource, t1.goods_id, nvl(t1.region_code, 'NALL'))

UNION ALL

SELECT
nvl(datasource, 'all') AS datasource,
nvl(nvl(temp.region_code, 'NALL'), 'all') AS region_code,
'search_list_product_detail' AS rec_page_code,
nvl(goods_id, 'all') AS goods_id,
0 AS sales_order,
0 AS gmv,
count(*) AS impressions,
0 AS clicks,
0 AS users
FROM (
SELECT
log.datasource,
log.geo_country AS region_code,
case when page_code in ('homepage','product_list') and list_type = '/product_list_newarrival' then 'rec_new_arrival'
     when page_code in ('homepage','product_list') and  list_type in ('/product_list_popular','/product_list') then 'rec_most_popular'
     when page_code in ('homepage','product_list') and  list_type = '/product_list_sold' then 'rec_sold'
     when page_code in ('homepage','product_list') and  list_type in ('/product_list_price_asc','/product_list_price_desc') then 'rec_price'
     when page_code ='product_detail' and list_type ='/detail_also_like' then 'rec_product_detail'
     when page_code ='search_result' and list_type in ('/search_result','/search_result_recommend') then 'rec_search_result'
     when page_code ='search_result' and list_type = '/search_result_sold' then 'rec_search_sold'
     when page_code ='search_result' and list_type in ('/search_result_price_desc','/search_result_price_asc') then 'rec_search_price'
     when page_code ='search_result' and list_type = '/search_result_newarrival' then 'rec_search_newarrival'
     else 'others' end rec_page_code,
dg.goods_id,
log.device_id
FROM dwd.dwd_vova_log_goods_impression log
inner join dim.dim_vova_goods dg on dg.virtual_goods_id = log.virtual_goods_id
INNER JOIN ods_vova_vtsf.ods_vova_acg_app a on a.data_domain = log.datasource
WHERE log.pt >= date_sub('${cur_date}', 6)
  AND log.pt <= '${cur_date}'
  AND log.platform = 'mob'
  
     ) temp
WHERE rec_page_code != 'others'
group by cube (datasource, goods_id, nvl(temp.region_code, 'NALL'))

UNION ALL

SELECT
nvl(datasource, 'all') AS datasource,
nvl(nvl(temp.region_code, 'NALL'), 'all') AS region_code,
'search_list_product_detail' AS rec_page_code,
nvl(goods_id, 'all') AS goods_id,
0 AS sales_order,
0 AS gmv,
0 AS impressions,
count(*) AS clicks,
count(distinct device_id) AS users
FROM (
SELECT
log.datasource,
log.geo_country AS region_code,
case when page_code in ('homepage','product_list') and list_type = '/product_list_newarrival' then 'rec_new_arrival'
     when page_code in ('homepage','product_list') and  list_type in ('/product_list_popular','/product_list') then 'rec_most_popular'
     when page_code in ('homepage','product_list') and  list_type = '/product_list_sold' then 'rec_sold'
     when page_code in ('homepage','product_list') and  list_type in ('/product_list_price_asc','/product_list_price_desc') then 'rec_price'
     when page_code ='product_detail' and list_type ='/detail_also_like' then 'rec_product_detail'
     when page_code ='search_result' and list_type in ('/search_result','/search_result_recommend') then 'rec_search_result'
     when page_code ='search_result' and list_type = '/search_result_sold' then 'rec_search_sold'
     when page_code ='search_result' and list_type in ('/search_result_price_desc','/search_result_price_asc') then 'rec_search_price'
     when page_code ='search_result' and list_type = '/search_result_newarrival' then 'rec_search_newarrival'
     else 'others' end rec_page_code,
dg.goods_id,
log.device_id
FROM dwd.dwd_vova_log_goods_click log
inner join dim.dim_vova_goods dg on dg.virtual_goods_id = log.virtual_goods_id
INNER JOIN ods_vova_vtsf.ods_vova_acg_app a on a.data_domain = log.datasource
WHERE log.pt >= date_sub('${cur_date}', 6)
  AND log.pt <= '${cur_date}'
  AND log.platform = 'mob'
  
     ) temp
WHERE rec_page_code != 'others'
group by cube (datasource, goods_id, nvl(temp.region_code, 'NALL'))
)
GROUP BY datasource, rec_page_code, goods_id, region_code
) t1
inner join dim.dim_vova_goods dg on dg.goods_id = t1.goods_id
INNER JOIN dim.dim_vova_merchant dm ON dm.mct_id = dg.mct_id
LEFT JOIN ods_vova_vts.ods_vova_brand b ON b.brand_id = dg.brand_id
left join dim.dim_vova_category c on dg.cat_id = c.cat_id
WHERE (t1.clicks > 0 OR t1.sales_order > 0)
AND t1.goods_id != 'all' AND t1.region_code IN ('all', 'FR', 'DE', 'IT', 'ES', 'GB', 'TW')

UNION ALL

SELECT
datasource,
rec_page_code,
platform,
region_code,
goods_id,
goods_sn,
sales_order,
gmv,
impressions,
clicks,
users,
ctr,
rate,
gr,
gcr,
last_update_time,
first_cat_name,
second_cat_name,
first_cat_id,
second_cat_id,
shop_price_amount,
is_on_sale,
brand_id,
brand_name,
mct_name,
mct_id,
third_cat_id,
third_cat_name,
fourth_cat_id,
fourth_cat_name
FROM
tmp.tmp_ads_vova_goods_performance_page_group

;
"

spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=ads_vova_goods_performance_page" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 280" \
--conf "spark.sql.shuffle.partitions=280" \
--conf "spark.dynamicAllocation.maxExecutors=180" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

