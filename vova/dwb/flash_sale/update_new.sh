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
-- flash_sale
INSERT OVERWRITE TABLE dwb.dwb_vova_daily_flash_sale_new PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
'${cur_date}',
t2.region_code,
t2.platform,
t1.market_paid_order_num,
t1.market_paid_buyer_num,
t1.market_order_order_num,
t1.market_order_user_num,
t1.flash_sale_goods_gmv,
t3.flash_sale_order_info_gmv,
t1.market_order_again_order_num,
t1.market_paid_again_order_num,
t2.market_gmv,
t4.on_sale_uv,
t4.upcoming_uv,
t8.cohort_1,
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
HAVING datasource = 'vova'
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
AND oi.pay_status >= 1
group by oi.user_id
) temp3 on temp3.user_id = dog.buyer_id

WHERE (date(dog.pay_time) = '${cur_date}' or date(dog.order_time) = '${cur_date}')
      AND dog.platform in ('ios', 'android')
      AND dog.parent_order_id = 0
      AND oc.pre_page_code in ('RNflashsale')
      AND oc.pt = '${cur_date}'
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
      AND dog.parent_order_id = 0
      AND oc.pre_page_code in ('RNflashsale')
      AND oc.pt = '${cur_date}'
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
count(upcoming_cnt) as upcoming_pv
from
(
    select
           nvl(geo_country, 'NALL') as region_code,
           nvl(os_type, 'NA') as platform,
           nvl(datasource, 'NA') as datasource,
           if(event_name = 'impressions' and page_code = 'RNflashsale' and lower(list_type) in ('/onsale', '/onasle'), device_id, null) as onsale_cnt,
           if(event_name = 'impressions' and page_code = 'RNflashsale' and lower(list_type) = '/upcoming' , device_id, null) as upcoming_cnt
    from
    (
select pt,datasource,'impressions' as event_name,geo_country,os_type,page_code,device_id,referrer,NULL view_type,element_name,NULL list_name,element_type,page_url,list_uri,list_type from dwd.dwd_vova_log_impressions_arc where pt='${cur_date}' AND event_type = 'normal' AND page_code = 'RNflashsale'
        ) t1
    ) log
group by cube(region_code,platform,datasource)
    ) t4 on t2.region_code = t4.region_code and t2.platform = t4.platform and t2.datasource = t4.datasource
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
    dwd.dwd_vova_log_impressions_arc log
    where pt = '${cur_date}'
      and page_code = 'RNflashsale'
      and lower(list_type) in ('/onsale', '/onasle')
)t1
group by region_code,platform,datasource,device_id
) a1
inner join  (
    select device_id,datasource
    from
    dwd.dwd_vova_log_impressions_arc log
    where pt = date_sub('${cur_date}', 1)
      and page_code = 'RNflashsale'
      and lower(list_type) in ('/onsale', '/onasle')
group by device_id,datasource
) a2 on a1.device_id = a2.device_id and a1.datasource = a2.datasource


group by cube(region_code,platform, a1.datasource)
    ) t8 on t2.region_code = t8.region_code and t2.platform = t8.platform and t2.datasource = t8.datasource
;

"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=20" --conf "spark.app.name=dwb_vova_daily_flash_sale_new" -e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

