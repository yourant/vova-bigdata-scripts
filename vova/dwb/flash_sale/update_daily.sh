#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

##flashsale
##dependence
#dwd.dwd_vova_fact_pay
#dwd.dwd_vova_fact_start_up
#dwd.dwd_vova_fact_order_cause_v2
#dwd.dwd_vova_log_screen_view
#dwd.dwd_vova_log_common_click
#dwd.dwd_vova_log_goods_click
#dwd.dwd_vova_log_goods_impression
#ads.ads_vova_flash_sale_goods_d

sql="
-- market
INSERT OVERWRITE TABLE dwb.dwb_vova_market_daily_flash_sale PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
'${cur_date}',
t2.region_code,
t2.platform,
t1.market_paid_order_num,
t1.market_paid_buyer_num,
t1.market_paid_goods_num,
t1.market_order_order_num,
t1.market_order_user_num,
t1.flash_sale_goods_gmv,
t3.flash_sale_order_info_gmv,
t1.market_order_again_order_num,
t1.market_paid_again_order_num,
t2.market_gmv,
t2.datasource
from
    (
    select sum(fp.shop_price * fp.goods_number + fp.shipping_fee) as market_gmv,
           nvl(fp.region_code, 'all') as region_code,
           nvl(fp.platform, 'all') as platform,
           nvl(fp.datasource, 'all') as datasource
    from
    dwd.dwd_vova_fact_pay fp
    where date(fp.pay_time) = '${cur_date}'
    AND fp.platform in ('ios', 'android')
    group by cube (fp.region_code, fp.platform, fp.datasource)
    ) t2 left join 
(
    select
nvl(region_code, 'all') as region_code,
nvl(platform, 'all') as platform,
nvl(datasource, 'all') as datasource,
count(distinct market_paid_order_num) as market_paid_order_num,
count(distinct market_paid_buyer_num) as market_paid_buyer_num,
count(distinct market_paid_goods_num) as market_paid_goods_num,
count(distinct market_order_order_num) as market_order_order_num,
count(distinct market_order_user_num) as market_order_user_num,
sum(flash_sale_goods_gmv) as flash_sale_goods_gmv,
count(distinct market_order_again_order_num) as market_order_again_order_num,
count(distinct market_paid_again_order_num) as market_paid_again_order_num
from (
     select
if(date(dog.pay_time) = '${cur_date}' and dog.pay_status >= 1, dog.order_id,null) as market_paid_order_num,
if(date(dog.pay_time) = '${cur_date}' and dog.pay_status >= 1, dog.buyer_id,null) as market_paid_buyer_num,
if(date(dog.pay_time) = '${cur_date}' and dog.pay_status >= 1, dog.goods_id,null) as market_paid_goods_num,
if(date(dog.order_time) = '${cur_date}', dog.order_id,null) as market_order_order_num,
if(date(dog.order_time) = '${cur_date}', dog.buyer_id,null) as market_order_user_num,
if(date(dog.pay_time) = '${cur_date}' and dog.pay_status >= 1, dog.shop_price * dog.goods_number + dog.shipping_fee,null) as flash_sale_goods_gmv,
if(date(dog.order_time) = '${cur_date}' and temp2.user_id is not null , dog.buyer_id,null) as market_order_again_order_num,
if(date(dog.pay_time) = '${cur_date}' and dog.pay_status >= 1 and temp3.user_id is not null , dog.buyer_id,null) as market_paid_again_order_num,
dog.platform,
dog.datasource,
dog.region_code

from
dim.dim_vova_order_goods dog
INNER JOIN ods_vova_vts.ods_vova_order_goods_extension oge ON oge.rec_id = dog.order_goods_id

LEFT JOIN (
SELECT oi.user_id
FROM ods_vova_vts.ods_vova_order_info oi
WHERE oi.order_time < '${cur_date}'
group by oi.user_id
) temp2 on temp2.user_id = dog.buyer_id

LEFT JOIN (
SELECT oi.user_id
FROM ods_vova_vts.ods_vova_order_info oi
WHERE oi.pay_time < '${cur_date}'
group by oi.user_id
) temp3 on temp3.user_id = dog.buyer_id

WHERE (date(dog.pay_time) = '${cur_date}' or date(dog.order_time) = '${cur_date}')
      AND oge.ext_name = 'is_flash_sale'
      AND dog.platform in ('ios', 'android')
      AND dog.parent_order_id = 0
         ) final
group by cube (final.region_code, final.platform, final.datasource)
) t1 on t1.region_code = t2.region_code and t1.platform = t2.platform and t1.datasource = t2.datasource
left join
    (
    select sum(fp.shop_price * fp.goods_number + fp.shipping_fee) as flash_sale_order_info_gmv,
           nvl(fp.region_code, 'all') as region_code,
           nvl(fp.datasource, 'all') as datasource,
           nvl(fp.platform, 'all') as platform
    from
    dwd.dwd_vova_fact_pay fp
    inner join (
    select distinct dog.order_id from
    dim.dim_vova_order_goods dog
INNER JOIN ods_vova_vts.ods_vova_order_goods_extension oge ON oge.rec_id = dog.order_goods_id
WHERE date(dog.pay_time) = '${cur_date}'
      AND dog.platform in ('ios', 'android')
      AND oge.ext_name = 'is_flash_sale'
      and dog.parent_order_id = 0
    ) tt1 on tt1.order_id = fp.order_id
    where date(fp.pay_time) = '${cur_date}'
    AND fp.platform in ('ios', 'android')
    group by cube (fp.region_code, fp.platform, fp.datasource)
    ) t3 on t1.region_code = t3.region_code and t1.platform = t3.platform and t1.datasource = t3.datasource
;

-- flash_sale
INSERT OVERWRITE TABLE dwb.dwb_vova_daily_flash_sale PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
'${cur_date}',
t2.region_code,
t2.platform,
t1.market_paid_order_num,
t1.market_paid_buyer_num,
t1.market_paid_goods_num,
t1.market_order_order_num,
t1.market_order_user_num,
t1.flash_sale_goods_gmv,
t3.flash_sale_order_info_gmv,
t1.market_order_again_order_num,
t1.market_paid_again_order_num,
t2.market_gmv,
t4.on_sale_uv,
t4.on_sale_pv,
t4.upcoming_uv,
t4.upcoming_pv,
t4.homepage_uv,
t4.onsale_produce_detail_cnt_uv,
t4.onsale_produce_detail_cnt_pv,
t4.upcoming_produce_detail_cnt_uv,
t4.upcoming_produce_detail_cnt_pv,
t4.list_add_bag_cnt_uv,
t4.list_add_bag_cnt_pv,
t5.product_detail_cnt_uv,
t5.product_detail_cnt_pv,
t6.click_uv,
t7.impression_uv,
t8.cohort_1,
t9.cohort_7,
t10.total_goods,
t11.cur_sale_flashsale_goods_cnt,
t2.market_dau,
t2.datasource
from
    (
select region_code  as region_code,
       platform     as platform,
       datasource   as datasource,
       sum(market_dau) as market_dau,
       sum(market_gmv) as market_gmv
from
(
    select sum(fp.shop_price * fp.goods_number + fp.shipping_fee) as market_gmv,
           0                                  as market_dau,
           nvl(fp.region_code, 'all') as region_code,
           nvl(fp.datasource, 'all') as datasource,
           nvl(fp.platform, 'all') as platform
    from
    dwd.dwd_vova_fact_pay fp
    where date(fp.pay_time) = '${cur_date}'
    AND fp.platform in ('ios', 'android')
    group by cube (fp.region_code, fp.platform, fp.datasource)
    union all
    select 0                                  as market_gmv,
           count(distinct device_id)          as market_dau,
           nvl(nvl(region_code, 'NALL'), 'all') as region_code,
           nvl(su.datasource, 'all') as datasource,
           nvl(nvl(platform, 'NA'), 'all')    as platform
    from dwd.dwd_vova_fact_start_up su
    where su.pt = '${cur_date}'
    group by cube (nvl(region_code, 'NALL'), nvl(platform, 'NA'), su.datasource)
) test
group by region_code, platform, datasource
    ) t2 left join
(
    select
nvl(region_code, 'all') as region_code,
nvl(platform, 'all') as platform,
nvl(datasource, 'all') as datasource,
count(distinct market_paid_order_num) as market_paid_order_num,
count(distinct market_paid_buyer_num) as market_paid_buyer_num,
count(distinct market_paid_goods_num) as market_paid_goods_num,
count(distinct market_order_order_num) as market_order_order_num,
count(distinct market_order_user_num) as market_order_user_num,
sum(flash_sale_goods_gmv) as flash_sale_goods_gmv,
count(distinct market_order_again_order_num) as market_order_again_order_num,
count(distinct market_paid_again_order_num) as market_paid_again_order_num
from (
     select
if(date(dog.pay_time) = '${cur_date}' and dog.pay_status >= 1, dog.order_id,null) as market_paid_order_num,
if(date(dog.pay_time) = '${cur_date}' and dog.pay_status >= 1, dog.buyer_id,null) as market_paid_buyer_num,
if(date(dog.pay_time) = '${cur_date}' and dog.pay_status >= 1, dog.goods_id,null) as market_paid_goods_num,
if(date(dog.order_time) = '${cur_date}', dog.order_id,null) as market_order_order_num,
if(date(dog.order_time) = '${cur_date}', dog.buyer_id,null) as market_order_user_num,
if(date(dog.pay_time) = '${cur_date}' and dog.pay_status >= 1, dog.shop_price * dog.goods_number + dog.shipping_fee,null) as flash_sale_goods_gmv,
if(date(dog.order_time) = '${cur_date}' and temp2.user_id is not null , dog.buyer_id,null) as market_order_again_order_num,
if(date(dog.pay_time) = '${cur_date}' and dog.pay_status >= 1 and temp3.user_id is not null , dog.buyer_id,null) as market_paid_again_order_num,
dog.platform,
dog.datasource,
dog.region_code

from
dim.dim_vova_order_goods dog
left join dwd.dwd_vova_fact_order_cause_v2 oc on dog.order_goods_id = oc.order_goods_id
LEFT JOIN (
SELECT oi.user_id
FROM ods_vova_vts.ods_vova_order_info oi
WHERE oi.order_time < '${cur_date}'
group by oi.user_id
) temp2 on temp2.user_id = dog.buyer_id

LEFT JOIN (
SELECT oi.user_id
FROM ods_vova_vts.ods_vova_order_info oi
WHERE oi.pay_time < '${cur_date}'
group by oi.user_id
) temp3 on temp3.user_id = dog.buyer_id

WHERE (date(dog.pay_time) = '${cur_date}' or date(dog.order_time) = '${cur_date}')
      AND dog.platform in ('ios', 'android')
      and dog.parent_order_id = 0
      AND oc.pre_page_code in ('h5flashsale','flashsale','h5flashsale_catlist','homepage')
      AND oc.pre_list_type in ('/hp_flashsale','/hp_flashsale/','/h5flashsale_category_list','/h5flashsale_main_list','/flashsale_2','/flashsale','/onsale','/upcoming','onsale','upcoming')
          and oc.pt = '${cur_date}'
         ) final
group by cube (final.region_code, final.platform, final.datasource)
) t1 on t1.region_code = t2.region_code and t1.platform = t2.platform and t1.datasource = t2.datasource
left join
    (
    select sum(fp.shop_price * fp.goods_number + fp.shipping_fee) as flash_sale_order_info_gmv,
           nvl(fp.region_code, 'all') as region_code,
           nvl(fp.datasource, 'all') as datasource,
           nvl(fp.platform, 'all') as platform
    from
    dwd.dwd_vova_fact_pay fp
    inner join (
    select distinct dog.order_id from
    dim.dim_vova_order_goods dog
    left join dwd.dwd_vova_fact_order_cause_v2 oc on dog.order_goods_id = oc.order_goods_id
WHERE date(dog.pay_time) = '${cur_date}'
      AND dog.platform in ('ios', 'android')
      and dog.parent_order_id = 0
      AND oc.pre_page_code in ('h5flashsale','flashsale','h5flashsale_catlist','homepage')
      AND oc.pre_list_type in ('/hp_flashsale','/hp_flashsale/','/h5flashsale_category_list','/h5flashsale_main_list','/flashsale_2','/flashsale','/onsale','/upcoming','onsale','upcoming')
      and oc.pt = '${cur_date}'
    ) tt1 on tt1.order_id = fp.order_id
    where date(fp.pay_time) = '${cur_date}'
    AND fp.platform in ('ios', 'android')
    group by cube (fp.region_code, fp.platform, fp.datasource)
    ) t3 on t2.region_code = t3.region_code and t2.platform = t3.platform and t2.datasource = t3.datasource



left join
(
select
nvl(region_code, 'all') as region_code,
nvl(platform, 'all') as platform,
nvl(datasource, 'all') as datasource,
count(distinct onsale_cnt) as on_sale_uv,
count(onsale_cnt) as on_sale_pv,
count(distinct upcoming_cnt) as upcoming_uv,
count(upcoming_cnt) as upcoming_pv,
count(homepage_cnt) as homepage_uv,
count(distinct onsale_produce_detail_cnt) as onsale_produce_detail_cnt_uv,
count(onsale_produce_detail_cnt) as onsale_produce_detail_cnt_pv,
count(distinct upcoming_produce_detail_cnt) as upcoming_produce_detail_cnt_uv,
count(upcoming_produce_detail_cnt) as upcoming_produce_detail_cnt_pv,
count(distinct list_add_bag_cnt) as list_add_bag_cnt_uv,
count(list_add_bag_cnt) as list_add_bag_cnt_pv
from
(
    select
           nvl(geo_country, 'NALL') as region_code,
           nvl(os_type, 'NA') as platform,
           nvl(datasource, 'NA') as datasource,
           if((event_name = 'common_click' and page_code = 'h5flashsale' and element_type = 'onsale') or (event_name = 'impressions' and page_code = 'flashsale' and list_type in ('onsale', '/onsale')), device_id, null) as onsale_cnt,
           if((event_name = 'common_click' and page_code = 'h5flashsale' and element_type = 'upcoming') or (event_name = 'impressions' and page_code = 'flashsale' and list_type in ('upcoming', '/upcoming')) , device_id, null) as upcoming_cnt,
           if(event_name = 'screen_view' and page_code = 'homepage', device_id, null) as homepage_cnt,
           if(event_name = 'screen_view' and page_code = 'product_detail' and referrer like '%flashsale%' and page_url like '%state=onsale&activity=flashsale&type=common', device_id, null) as onsale_produce_detail_cnt,
           if(event_name = 'screen_view' and page_code = 'product_detail' and referrer like '%flashsale%' and page_url like '%state=upcoming&activity=flashsale&type=common', device_id, null) as upcoming_produce_detail_cnt,
           if((event_name = 'common_click' and element_name in ('h5flashsaleListadd2cartIcon') and list_uri = 'h5flashsale_click') or (event_name = 'click' and page_code = 'flashsale' and element_name = 'flashsaleSkuPopupBuynowButton'), device_id, null) as list_add_bag_cnt
    from
    (
select pt,datasource,event_name,geo_country,os_type,page_code,device_id,referrer,view_type,NULL element_name,NULL list_name,NULL element_type,page_url,NULL list_uri,NULL list_type from dwd.dwd_vova_log_screen_view where pt='${cur_date}'
UNION
select pt,datasource,event_name,geo_country,os_type,page_code,device_id,referrer,NULL view_type,element_name,list_name,element_type,page_url,list_uri,NULL list_type from dwd.dwd_vova_log_common_click where pt='${cur_date}'
UNION
select pt,datasource,'click' as event_name,geo_country,os_type,page_code,device_id,referrer,NULL view_type,element_name,NULL list_name,element_type,page_url,list_uri,list_type from dwd.dwd_vova_log_click_arc where pt='${cur_date}' AND event_type = 'normal'
UNION
select pt,datasource,'impressions' as event_name,geo_country,os_type,page_code,device_id,referrer,NULL view_type,element_name,NULL list_name,element_type,page_url,list_uri,list_type from dwd.dwd_vova_log_impressions_arc where pt='${cur_date}' AND event_type = 'normal'
        ) t1
    ) log
group by cube(region_code,platform,datasource)
    ) t4 on t2.region_code = t4.region_code and t2.platform = t4.platform and t2.datasource = t4.datasource

left join
(

    select
           nvl(nvl(geo_country,'NALL'), 'all') as region_code,
           nvl(nvl(os_type,'NA'), 'all') as platform,
           nvl(nvl(datasource,'NA'), 'all') as datasource,
           count(distinct device_id) as product_detail_cnt_uv,
           count(device_id) as product_detail_cnt_pv
    from
    dwd.dwd_vova_log_goods_click log
    where page_code in ('h5flashsale', 'flashsale', 'h5flashsale_catlist') and pt = '${cur_date}'
group by cube(nvl(geo_country,'NALL'),nvl(os_type,'NA'),nvl(datasource,'NA'))
    ) t5 on t2.region_code = t5.region_code and t2.platform = t5.platform and t2.datasource = t5.datasource

left join
(

    select
           nvl(nvl(geo_country,'NALL'), 'all') as region_code,
           nvl(nvl(os_type,'NA'), 'all') as platform,
           nvl(nvl(datasource,'NA'), 'all') as datasource,
           count(distinct device_id) as click_uv
    from
    dwd.dwd_vova_log_goods_click log
    where page_code in ('h5flashsale', 'flashsale', 'h5flashsale_catlist') and pt = '${cur_date}'
group by cube(nvl(geo_country,'NALL'),nvl(os_type,'NA'),nvl(datasource,'NA'))
    ) t6 on t2.region_code = t6.region_code and t2.platform = t6.platform and t2.datasource = t6.datasource

left join
(

    select
           nvl(nvl(geo_country,'NALL'), 'all') as region_code,
           nvl(nvl(os_type,'NA'), 'all') as platform,
           nvl(nvl(datasource,'NA'), 'all') as datasource,
           count(distinct device_id) as impression_uv
    from
    dwd.dwd_vova_log_goods_impression log
    where page_code in ('h5flashsale', 'flashsale', 'h5flashsale_catlist') and pt = '${cur_date}'
group by cube(nvl(geo_country,'NALL'),nvl(os_type,'NA'),nvl(datasource,'NA'))
    ) t7 on t2.region_code = t7.region_code and t2.platform = t7.platform and t2.datasource = t7.datasource

left join
(

select
           nvl(region_code,'all') as region_code,
           nvl(platform,'all') as platform,
           nvl(a1.datasource,'all') as datasource,
count(distinct a1.device_id) as cohort_1
from
(
select
region_code,
platform,
datasource,
device_id
from
(
    select
           nvl(geo_country,'NALL') as region_code,
           nvl(os_type,'NA') as platform,
           nvl(datasource,'NA') as datasource,
           device_id
    from
    dwd.dwd_vova_log_common_click log
    where pt = '${cur_date}'
and page_code = 'h5flashsale' and element_type = 'onsale'
union
    select
           nvl(geo_country,'NALL') as region_code,
           nvl(os_type,'NA') as platform,
           nvl(datasource,'NA') as datasource,
           device_id
    from
    dwd.dwd_vova_log_click_arc log
    where pt = '${cur_date}'
and page_code = 'flashsale' and list_type in ('onsale', '/onsale') AND event_type = 'normal'
)t1
group by region_code,platform,datasource,device_id
) a1
inner join  (
    select device_id,datasource
    from
    dwd.dwd_vova_log_common_click log
    where pt = date_sub('${cur_date}', 1)
and page_code = 'h5flashsale' and element_type = 'onsale'
group by device_id,datasource
union
    select device_id,datasource
    from
    dwd.dwd_vova_log_click_arc log
    where pt = date_sub('${cur_date}', 1)
and page_code = 'flashsale' and list_type in ('onsale', '/onsale') AND event_type = 'normal'
group by device_id,datasource
) a2 on a1.device_id = a2.device_id and a1.datasource = a2.datasource


group by cube(region_code,platform, a1.datasource)
    ) t8 on t2.region_code = t8.region_code and t2.platform = t8.platform and t2.datasource = t8.datasource
left join
(

select
           nvl(region_code,'all') as region_code,
           nvl(platform,'all') as platform,
           nvl(a1.datasource,'all') as datasource,
count(distinct a1.device_id) as cohort_7
from
(
select
region_code,
platform,
datasource,
device_id
from
(
    select
           nvl(geo_country,'NALL') as region_code,
           nvl(os_type,'NA') as platform,
           nvl(datasource,'NA') as datasource,
           device_id
    from
    dwd.dwd_vova_log_common_click log
    where pt = '${cur_date}'
and page_code = 'h5flashsale' and element_type = 'onsale'
union
    select
           nvl(geo_country,'NALL') as region_code,
           nvl(os_type,'NA') as platform,
           nvl(datasource,'NA') as datasource,
           device_id
    from
    dwd.dwd_vova_log_click_arc log
    where pt = '${cur_date}'
and page_code = 'flashsale' and list_type in ('onsale', '/onsale') AND event_type = 'normal'
)t1
group by region_code,platform,datasource,device_id
) a1
inner join  (
    select device_id,datasource
    from
    dwd.dwd_vova_log_common_click log
    where pt = date_sub('${cur_date}', 7)
and page_code = 'h5flashsale' and element_type = 'onsale'
group by device_id,datasource
union
    select device_id,datasource
    from
    dwd.dwd_vova_log_click_arc log
    where pt = date_sub('${cur_date}', 7)
and page_code = 'flashsale' and list_type in ('onsale', '/onsale') AND event_type = 'normal'
group by device_id,datasource
) a2 on a1.device_id = a2.device_id and a1.datasource = a2.datasource


group by cube(region_code,platform, a1.datasource)
    ) t9 on t2.region_code = t9.region_code and t2.platform = t9.platform and t2.datasource = t9.datasource

left join (

SELECT
  count(DISTINCT afsg.goods_id) AS total_goods,
  'vova' as datasource,
  'all' as platform,
  'all' as region_code
FROM ads.ads_vova_flash_sale_goods_d afsg
WHERE afsg.pt >= date_sub('${cur_date}', 4)
AND afsg.flash_sale_date = '${cur_date}'
UNION ALL
SELECT
  count(DISTINCT ac.goods_id) AS total_goods,
  'airyclub' as datasource,
  'all' as platform,
  'all' as region_code
FROM ods_vova_vts.ods_vova_activity a
INNER JOIN ods_vova_vts.ods_vova_activity_coupon ac on a.activity_id = ac.activity_id
WHERE date(a.activity_start_time) = '${cur_date}'
AND a.is_delete=0
AND a.parent_activity_id = 411385
) t10 on t2.region_code = t10.region_code and t2.platform = t10.platform and t2.datasource = t10.datasource

left join (

SELECT count(DISTINCT fp.goods_id) as cur_sale_flashsale_goods_cnt,
  'all' as platform,
  nvl(fp.datasource, 'all') as datasource,
  'all' as region_code
FROM dwd.dwd_vova_fact_pay fp
         INNER JOIN ods_vova_vts.ods_vova_order_goods_extension oge ON fp.order_goods_id = oge.rec_id
WHERE date(fp.order_time) = '${cur_date}'
  AND oge.ext_name = 'is_flash_sale'
group by cube(fp.datasource)
) t11 on t2.region_code = t11.region_code and t2.platform = t11.platform and t2.datasource = t11.datasource
;


INSERT OVERWRITE TABLE dwb.dwb_vova_flash_sale_data PARTITION (pt = '${cur_date}')
select /*+ REPARTITION(1) */
t1.datasource,
t1.goods_id,
'${cur_date}' AS sale_date,
dg.virtual_goods_id,
dg.first_cat_name,
dg.second_cat_name,
dg.shop_price,
dg.shipping_fee,
t2.order_user,
t2.order_num,
t3.pay_user,
t3.pay_num,
t3.gmv,
dg.goods_name,
dg.shop_price + dg.shipping_fee as shop_price_amount,
nvl(t3.gmv / t3.pay_user, 0) as gmv_div_pay_user
from
(
select
'vova' as datasource,
afsg.goods_id
FROM ads.ads_vova_flash_sale_goods_d afsg
WHERE afsg.pt >= date_sub('${cur_date}', 4)
AND afsg.flash_sale_date = '${cur_date}'
GROUP BY afsg.goods_id

UNION ALL

select
'airyclub' as datasource,
ac.goods_id
FROM ods_vova_vts.ods_vova_activity a
INNER JOIN ods_vova_vts.ods_vova_activity_coupon ac on a.activity_id = ac.activity_id
WHERE date(a.activity_start_time) = '${cur_date}'
AND a.is_delete=0
AND a.parent_activity_id = 411385
GROUP BY ac.goods_id
)t1
left join
(
SELECT
  dog.datasource,
  dog.goods_id,
  count(DISTINCT dog.order_id) AS order_num,
  count(DISTINCT dog.buyer_id)  AS order_user
FROM dim.dim_vova_order_goods dog
  INNER JOIN ods_vova_vts.ods_vova_order_goods_extension oge ON oge.rec_id = dog.order_goods_id
WHERE date(dog.order_time) = '${cur_date}'
      AND oge.ext_name = 'is_flash_sale'
      AND dog.parent_order_id = 0
GROUP BY dog.goods_id, dog.datasource
) t2 on t1.goods_id = t2.goods_id and t1.datasource = t2.datasource
left join
(
SELECT
  dog.datasource,
  dog.goods_id,
  sum(dog.shop_price * dog.goods_number + dog.shipping_fee) AS gmv,
  count(DISTINCT dog.order_id) AS pay_num,
  count(DISTINCT dog.buyer_id)  AS pay_user
FROM dim.dim_vova_order_goods dog
  INNER JOIN ods_vova_vts.ods_vova_order_goods_extension oge ON oge.rec_id = dog.order_goods_id
WHERE date(dog.pay_time) = '${cur_date}'
      AND oge.ext_name = 'is_flash_sale'
      AND dog.pay_status >= 1
      AND dog.parent_order_id = 0
GROUP BY dog.goods_id, dog.datasource
) t3 on t1.goods_id = t3.goods_id and t1.datasource = t3.datasource
left join dim.dim_vova_goods dg on dg.goods_id = t1.goods_id
;

-- flash_sale_hour
INSERT OVERWRITE TABLE dwb.dwb_vova_flash_sale_per_hour PARTITION (pt = '${cur_date}')
SELECT /*+ REPARTITION(1) */
       '${cur_date}',
       nvl(temp.region_code, 'all') AS region_code,
       nvl(temp.platform, 'all')    AS platform,
       'flash_sale_gmv' AS activity_name,
       'FlashSale商品订单GMV' AS field_name,
       sum(hour_0)                AS hour_0,
       sum(hour_1)                AS hour_1,
       sum(hour_2)                AS hour_2,
       sum(hour_3)                AS hour_3,
       sum(hour_4)                AS hour_4,
       sum(hour_5)                AS hour_5,
       sum(hour_6)                AS hour_6,
       sum(hour_7)                AS hour_7,
       sum(hour_8)                AS hour_8,
       sum(hour_9)                AS hour_9,
       sum(hour_10)               AS hour_10,
       sum(hour_11)               AS hour_11,
       sum(hour_12)               AS hour_12,
       sum(hour_13)               AS hour_13,
       sum(hour_14)               AS hour_14,
       sum(hour_15)               AS hour_15,
       sum(hour_16)               AS hour_16,
       sum(hour_17)               AS hour_17,
       sum(hour_18)               AS hour_18,
       sum(hour_19)               AS hour_19,
       sum(hour_20)               AS hour_20,
       sum(hour_21)               AS hour_21,
       sum(hour_22)               AS hour_22,
       sum(hour_23)               AS hour_23
FROM (
         SELECT nvl(fp.region_code, 'NALL')                        AS region_code,
                nvl(fp.platform, 'NA')                             AS platform,
                date(fp.pay_time)                              AS event_date,
                if(hour(fp.pay_time) = 0, fp.shipping_fee + fp.shop_price * fp.goods_number, 0)  AS hour_0,
                if(hour(fp.pay_time) = 1, fp.shipping_fee + fp.shop_price * fp.goods_number, 0)  AS hour_1,
                if(hour(fp.pay_time) = 2, fp.shipping_fee + fp.shop_price * fp.goods_number, 0)  AS hour_2,
                if(hour(fp.pay_time) = 3, fp.shipping_fee + fp.shop_price * fp.goods_number, 0)  AS hour_3,
                if(hour(fp.pay_time) = 4, fp.shipping_fee + fp.shop_price * fp.goods_number, 0)  AS hour_4,
                if(hour(fp.pay_time) = 5, fp.shipping_fee + fp.shop_price * fp.goods_number, 0)  AS hour_5,
                if(hour(fp.pay_time) = 6, fp.shipping_fee + fp.shop_price * fp.goods_number, 0)  AS hour_6,
                if(hour(fp.pay_time) = 7, fp.shipping_fee + fp.shop_price * fp.goods_number, 0)  AS hour_7,
                if(hour(fp.pay_time) = 8, fp.shipping_fee + fp.shop_price * fp.goods_number, 0)  AS hour_8,
                if(hour(fp.pay_time) = 9, fp.shipping_fee + fp.shop_price * fp.goods_number, 0)  AS hour_9,
                if(hour(fp.pay_time) = 10, fp.shipping_fee + fp.shop_price * fp.goods_number, 0) AS hour_10,
                if(hour(fp.pay_time) = 11, fp.shipping_fee + fp.shop_price * fp.goods_number, 0) AS hour_11,
                if(hour(fp.pay_time) = 12, fp.shipping_fee + fp.shop_price * fp.goods_number, 0) AS hour_12,
                if(hour(fp.pay_time) = 13, fp.shipping_fee + fp.shop_price * fp.goods_number, 0) AS hour_13,
                if(hour(fp.pay_time) = 14, fp.shipping_fee + fp.shop_price * fp.goods_number, 0) AS hour_14,
                if(hour(fp.pay_time) = 15, fp.shipping_fee + fp.shop_price * fp.goods_number, 0) AS hour_15,
                if(hour(fp.pay_time) = 16, fp.shipping_fee + fp.shop_price * fp.goods_number, 0) AS hour_16,
                if(hour(fp.pay_time) = 17, fp.shipping_fee + fp.shop_price * fp.goods_number, 0) AS hour_17,
                if(hour(fp.pay_time) = 18, fp.shipping_fee + fp.shop_price * fp.goods_number, 0) AS hour_18,
                if(hour(fp.pay_time) = 19, fp.shipping_fee + fp.shop_price * fp.goods_number, 0) AS hour_19,
                if(hour(fp.pay_time) = 20, fp.shipping_fee + fp.shop_price * fp.goods_number, 0) AS hour_20,
                if(hour(fp.pay_time) = 21, fp.shipping_fee + fp.shop_price * fp.goods_number, 0) AS hour_21,
                if(hour(fp.pay_time) = 22, fp.shipping_fee + fp.shop_price * fp.goods_number, 0) AS hour_22,
                if(hour(fp.pay_time) = 23, fp.shipping_fee + fp.shop_price * fp.goods_number, 0) AS hour_23
         FROM dwd.dwd_vova_fact_pay fp
           inner join ods_vova_vts.ods_vova_order_goods_extension oge on fp.order_goods_id = oge.rec_id
         WHERE fp.datasource = 'vova'
           AND date(fp.pay_time) = '${cur_date}'
           AND oge.ext_name = 'is_flash_sale'
           ) temp
GROUP BY cube(temp.region_code, temp.platform)
UNION
SELECT '${cur_date}',
       nvl(temp.region_code, 'all') AS region_code,
       nvl(temp.platform, 'all')    AS platform,
       'flash_sale_pay_user' AS activity_name,
       'FlashSale商品订单支付成功人数' AS field_name,
       count(distinct hour_0)                AS hour_0,
       count(distinct hour_1)                AS hour_1,
       count(distinct hour_2)                AS hour_2,
       count(distinct hour_3)                AS hour_3,
       count(distinct hour_4)                AS hour_4,
       count(distinct hour_5)                AS hour_5,
       count(distinct hour_6)                AS hour_6,
       count(distinct hour_7)                AS hour_7,
       count(distinct hour_8)                AS hour_8,
       count(distinct hour_9)                AS hour_9,
       count(distinct hour_10)               AS hour_10,
       count(distinct hour_11)               AS hour_11,
       count(distinct hour_12)               AS hour_12,
       count(distinct hour_13)               AS hour_13,
       count(distinct hour_14)               AS hour_14,
       count(distinct hour_15)               AS hour_15,
       count(distinct hour_16)               AS hour_16,
       count(distinct hour_17)               AS hour_17,
       count(distinct hour_18)               AS hour_18,
       count(distinct hour_19)               AS hour_19,
       count(distinct hour_20)               AS hour_20,
       count(distinct hour_21)               AS hour_21,
       count(distinct hour_22)               AS hour_22,
       count(distinct hour_23)               AS hour_23
FROM (
         SELECT nvl(fp.region_code, 'NALL')                        AS region_code,
                nvl(fp.platform, 'NA')                             AS platform,
                date(fp.pay_time)                              AS event_date,
                if(hour(fp.pay_time) = 0, fp.buyer_id, NULL)  AS hour_0,
                if(hour(fp.pay_time) = 1, fp.buyer_id, NULL)  AS hour_1,
                if(hour(fp.pay_time) = 2, fp.buyer_id, NULL)  AS hour_2,
                if(hour(fp.pay_time) = 3, fp.buyer_id, NULL)  AS hour_3,
                if(hour(fp.pay_time) = 4, fp.buyer_id, NULL)  AS hour_4,
                if(hour(fp.pay_time) = 5, fp.buyer_id, NULL)  AS hour_5,
                if(hour(fp.pay_time) = 6, fp.buyer_id, NULL)  AS hour_6,
                if(hour(fp.pay_time) = 7, fp.buyer_id, NULL)  AS hour_7,
                if(hour(fp.pay_time) = 8, fp.buyer_id, NULL)  AS hour_8,
                if(hour(fp.pay_time) = 9, fp.buyer_id, NULL)  AS hour_9,
                if(hour(fp.pay_time) = 10, fp.buyer_id, NULL) AS hour_10,
                if(hour(fp.pay_time) = 11, fp.buyer_id, NULL) AS hour_11,
                if(hour(fp.pay_time) = 12, fp.buyer_id, NULL) AS hour_12,
                if(hour(fp.pay_time) = 13, fp.buyer_id, NULL) AS hour_13,
                if(hour(fp.pay_time) = 14, fp.buyer_id, NULL) AS hour_14,
                if(hour(fp.pay_time) = 15, fp.buyer_id, NULL) AS hour_15,
                if(hour(fp.pay_time) = 16, fp.buyer_id, NULL) AS hour_16,
                if(hour(fp.pay_time) = 17, fp.buyer_id, NULL) AS hour_17,
                if(hour(fp.pay_time) = 18, fp.buyer_id, NULL) AS hour_18,
                if(hour(fp.pay_time) = 19, fp.buyer_id, NULL) AS hour_19,
                if(hour(fp.pay_time) = 20, fp.buyer_id, NULL) AS hour_20,
                if(hour(fp.pay_time) = 21, fp.buyer_id, NULL) AS hour_21,
                if(hour(fp.pay_time) = 22, fp.buyer_id, NULL) AS hour_22,
                if(hour(fp.pay_time) = 23, fp.buyer_id, NULL) AS hour_23
         FROM dwd.dwd_vova_fact_pay fp
           inner join ods_vova_vts.ods_vova_order_goods_extension oge on fp.order_goods_id = oge.rec_id
         WHERE fp.datasource = 'vova'
           AND date(fp.pay_time) = '${cur_date}'
           AND oge.ext_name = 'is_flash_sale'
           ) temp
GROUP BY cube(temp.region_code, temp.platform)

-- step3
UNION
SELECT '${cur_date}',
       nvl(temp.region_code, 'all') AS region_code,
       nvl(temp.platform, 'all')    AS platform,
       'flash_sale_order_user' AS activity_name,
       'FlashSale商品订单下单人数' AS field_name,
       count(distinct hour_0)                AS hour_0,
       count(distinct hour_1)                AS hour_1,
       count(distinct hour_2)                AS hour_2,
       count(distinct hour_3)                AS hour_3,
       count(distinct hour_4)                AS hour_4,
       count(distinct hour_5)                AS hour_5,
       count(distinct hour_6)                AS hour_6,
       count(distinct hour_7)                AS hour_7,
       count(distinct hour_8)                AS hour_8,
       count(distinct hour_9)                AS hour_9,
       count(distinct hour_10)               AS hour_10,
       count(distinct hour_11)               AS hour_11,
       count(distinct hour_12)               AS hour_12,
       count(distinct hour_13)               AS hour_13,
       count(distinct hour_14)               AS hour_14,
       count(distinct hour_15)               AS hour_15,
       count(distinct hour_16)               AS hour_16,
       count(distinct hour_17)               AS hour_17,
       count(distinct hour_18)               AS hour_18,
       count(distinct hour_19)               AS hour_19,
       count(distinct hour_20)               AS hour_20,
       count(distinct hour_21)               AS hour_21,
       count(distinct hour_22)               AS hour_22,
       count(distinct hour_23)               AS hour_23
FROM (
         SELECT nvl(dog.region_code, 'NALL')                        AS region_code,
                nvl(dog.platform, 'NA')                             AS platform,
                date(dog.order_time)                              AS event_date,
                if(hour(dog.order_time) = 0, dog.buyer_id, NULL)  AS hour_0,
                if(hour(dog.order_time) = 1, dog.buyer_id, NULL)  AS hour_1,
                if(hour(dog.order_time) = 2, dog.buyer_id, NULL)  AS hour_2,
                if(hour(dog.order_time) = 3, dog.buyer_id, NULL)  AS hour_3,
                if(hour(dog.order_time) = 4, dog.buyer_id, NULL)  AS hour_4,
                if(hour(dog.order_time) = 5, dog.buyer_id, NULL)  AS hour_5,
                if(hour(dog.order_time) = 6, dog.buyer_id, NULL)  AS hour_6,
                if(hour(dog.order_time) = 7, dog.buyer_id, NULL)  AS hour_7,
                if(hour(dog.order_time) = 8, dog.buyer_id, NULL)  AS hour_8,
                if(hour(dog.order_time) = 9, dog.buyer_id, NULL)  AS hour_9,
                if(hour(dog.order_time) = 10, dog.buyer_id, NULL) AS hour_10,
                if(hour(dog.order_time) = 11, dog.buyer_id, NULL) AS hour_11,
                if(hour(dog.order_time) = 12, dog.buyer_id, NULL) AS hour_12,
                if(hour(dog.order_time) = 13, dog.buyer_id, NULL) AS hour_13,
                if(hour(dog.order_time) = 14, dog.buyer_id, NULL) AS hour_14,
                if(hour(dog.order_time) = 15, dog.buyer_id, NULL) AS hour_15,
                if(hour(dog.order_time) = 16, dog.buyer_id, NULL) AS hour_16,
                if(hour(dog.order_time) = 17, dog.buyer_id, NULL) AS hour_17,
                if(hour(dog.order_time) = 18, dog.buyer_id, NULL) AS hour_18,
                if(hour(dog.order_time) = 19, dog.buyer_id, NULL) AS hour_19,
                if(hour(dog.order_time) = 20, dog.buyer_id, NULL) AS hour_20,
                if(hour(dog.order_time) = 21, dog.buyer_id, NULL) AS hour_21,
                if(hour(dog.order_time) = 22, dog.buyer_id, NULL) AS hour_22,
                if(hour(dog.order_time) = 23, dog.buyer_id, NULL) AS hour_23
         FROM dim.dim_vova_order_goods dog
           inner join ods_vova_vts.ods_vova_order_goods_extension oge on dog.order_goods_id = oge.rec_id
         WHERE dog.datasource = 'vova'
           AND date(dog.order_time) = '${cur_date}'
           AND oge.ext_name = 'is_flash_sale'
           ) temp
GROUP BY cube(temp.region_code, temp.platform)

-- step4
UNION
SELECT '${cur_date}',
       nvl(temp.region_code, 'all') AS region_code,
       nvl(temp.platform, 'all')    AS platform,
       'flash_sale_on_sale' AS activity_name,
       'FlashSale主会场曝光UV' AS field_name,
       count(distinct hour_0)                AS hour_0,
       count(distinct hour_1)                AS hour_1,
       count(distinct hour_2)                AS hour_2,
       count(distinct hour_3)                AS hour_3,
       count(distinct hour_4)                AS hour_4,
       count(distinct hour_5)                AS hour_5,
       count(distinct hour_6)                AS hour_6,
       count(distinct hour_7)                AS hour_7,
       count(distinct hour_8)                AS hour_8,
       count(distinct hour_9)                AS hour_9,
       count(distinct hour_10)               AS hour_10,
       count(distinct hour_11)               AS hour_11,
       count(distinct hour_12)               AS hour_12,
       count(distinct hour_13)               AS hour_13,
       count(distinct hour_14)               AS hour_14,
       count(distinct hour_15)               AS hour_15,
       count(distinct hour_16)               AS hour_16,
       count(distinct hour_17)               AS hour_17,
       count(distinct hour_18)               AS hour_18,
       count(distinct hour_19)               AS hour_19,
       count(distinct hour_20)               AS hour_20,
       count(distinct hour_21)               AS hour_21,
       count(distinct hour_22)               AS hour_22,
       count(distinct hour_23)               AS hour_23
FROM (
         SELECT nvl(geo_country, 'NALL')                        AS region_code,
                nvl(os_type, 'NA')                             AS platform,
                pt                              AS event_date,
                if(log_hour = 0, device_id, NULL)  AS hour_0,
                if(log_hour = 1, device_id, NULL)  AS hour_1,
                if(log_hour = 2, device_id, NULL)  AS hour_2,
                if(log_hour = 3, device_id, NULL)  AS hour_3,
                if(log_hour = 4, device_id, NULL)  AS hour_4,
                if(log_hour = 5, device_id, NULL)  AS hour_5,
                if(log_hour = 6, device_id, NULL)  AS hour_6,
                if(log_hour = 7, device_id, NULL)  AS hour_7,
                if(log_hour = 8, device_id, NULL)  AS hour_8,
                if(log_hour = 9, device_id, NULL)  AS hour_9,
                if(log_hour = 10, device_id, NULL) AS hour_10,
                if(log_hour = 11, device_id, NULL) AS hour_11,
                if(log_hour = 12, device_id, NULL) AS hour_12,
                if(log_hour = 13, device_id, NULL) AS hour_13,
                if(log_hour = 14, device_id, NULL) AS hour_14,
                if(log_hour = 15, device_id, NULL) AS hour_15,
                if(log_hour = 16, device_id, NULL) AS hour_16,
                if(log_hour = 17, device_id, NULL) AS hour_17,
                if(log_hour = 18, device_id, NULL) AS hour_18,
                if(log_hour = 19, device_id, NULL) AS hour_19,
                if(log_hour = 20, device_id, NULL) AS hour_20,
                if(log_hour = 21, device_id, NULL) AS hour_21,
                if(log_hour = 22, device_id, NULL) AS hour_22,
                if(log_hour = 23, device_id, NULL) AS hour_23
         FROM (
         SELECT log.pt,
                log.device_id,
                log.geo_country,
                log.os_type,
                hour(from_unixtime(cast(log.collector_tstamp / 1000 AS int))) AS log_hour
         FROM
         dwd.dwd_vova_log_common_click log
         WHERE log.datasource = 'vova'
           AND log.pt = '${cur_date}'
           AND page_code in ('h5flashsale', 'h5flashsale_catlist')
           AND element_type = 'onsale'
           UNION
         SELECT log.pt,
                log.device_id,
                log.geo_country,
                log.os_type,
                hour(log.collector_ts) AS log_hour
         FROM
              dwd.dwd_vova_log_click_arc log
         WHERE log.datasource = 'vova'
           AND log.pt = '${cur_date}'
           AND log.page_code in ('flashsale')
           AND log.list_type in ('onsale', '/onsale')
           AND log.event_type = 'normal'
         ) temp1
           ) temp
GROUP BY cube(temp.region_code, temp.platform)

-- step5
UNION
SELECT '${cur_date}',
       nvl(temp.region_code, 'all') AS region_code,
       nvl(temp.platform, 'all')    AS platform,
       'flash_sale_product_detail' AS activity_name,
       'FlashSale商详页曝光UV' AS field_name,
       count(distinct hour_0)                AS hour_0,
       count(distinct hour_1)                AS hour_1,
       count(distinct hour_2)                AS hour_2,
       count(distinct hour_3)                AS hour_3,
       count(distinct hour_4)                AS hour_4,
       count(distinct hour_5)                AS hour_5,
       count(distinct hour_6)                AS hour_6,
       count(distinct hour_7)                AS hour_7,
       count(distinct hour_8)                AS hour_8,
       count(distinct hour_9)                AS hour_9,
       count(distinct hour_10)               AS hour_10,
       count(distinct hour_11)               AS hour_11,
       count(distinct hour_12)               AS hour_12,
       count(distinct hour_13)               AS hour_13,
       count(distinct hour_14)               AS hour_14,
       count(distinct hour_15)               AS hour_15,
       count(distinct hour_16)               AS hour_16,
       count(distinct hour_17)               AS hour_17,
       count(distinct hour_18)               AS hour_18,
       count(distinct hour_19)               AS hour_19,
       count(distinct hour_20)               AS hour_20,
       count(distinct hour_21)               AS hour_21,
       count(distinct hour_22)               AS hour_22,
       count(distinct hour_23)               AS hour_23
FROM (
         SELECT nvl(geo_country, 'NALL')                        AS region_code,
                nvl(os_type, 'NA')                             AS platform,
                pt                              AS event_date,
                if(log_hour = 0, device_id, NULL)  AS hour_0,
                if(log_hour = 1, device_id, NULL)  AS hour_1,
                if(log_hour = 2, device_id, NULL)  AS hour_2,
                if(log_hour = 3, device_id, NULL)  AS hour_3,
                if(log_hour = 4, device_id, NULL)  AS hour_4,
                if(log_hour = 5, device_id, NULL)  AS hour_5,
                if(log_hour = 6, device_id, NULL)  AS hour_6,
                if(log_hour = 7, device_id, NULL)  AS hour_7,
                if(log_hour = 8, device_id, NULL)  AS hour_8,
                if(log_hour = 9, device_id, NULL)  AS hour_9,
                if(log_hour = 10, device_id, NULL) AS hour_10,
                if(log_hour = 11, device_id, NULL) AS hour_11,
                if(log_hour = 12, device_id, NULL) AS hour_12,
                if(log_hour = 13, device_id, NULL) AS hour_13,
                if(log_hour = 14, device_id, NULL) AS hour_14,
                if(log_hour = 15, device_id, NULL) AS hour_15,
                if(log_hour = 16, device_id, NULL) AS hour_16,
                if(log_hour = 17, device_id, NULL) AS hour_17,
                if(log_hour = 18, device_id, NULL) AS hour_18,
                if(log_hour = 19, device_id, NULL) AS hour_19,
                if(log_hour = 20, device_id, NULL) AS hour_20,
                if(log_hour = 21, device_id, NULL) AS hour_21,
                if(log_hour = 22, device_id, NULL) AS hour_22,
                if(log_hour = 23, device_id, NULL) AS hour_23
         FROM (
         SELECT log.pt,
                log.device_id,
                log.geo_country,
                log.os_type,
                hour(from_unixtime(cast(log.collector_tstamp / 1000 AS int))) AS log_hour
         FROM
         dwd.dwd_vova_log_goods_click log
         WHERE log.datasource = 'vova'
           AND log.pt = '${cur_date}'
           AND page_code in ('h5flashsale', 'flashsale', 'h5flashsale_catlist')
         ) temp1
           ) temp
GROUP BY cube(temp.region_code, temp.platform)
;
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=20" --conf "spark.app.name=dwb_vova_flash_sale" -e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

