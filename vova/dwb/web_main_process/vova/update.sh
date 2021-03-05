#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

sql="
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE dwb.dwb_vova_web_main_process PARTITION (pt)
select
/*+ REPARTITION(1) */
fin.event_date,
fin.datasource,
fin.region_code,
fin.platform,
fin.original_channel,
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
fin.event_date AS pt
from
(
select
nvl(log_data.datasource,'all') datasource,
nvl(log_data.pt,'all') event_date,
nvl(log_data.region_code,'all') region_code,
nvl(log_data.platform,'all') platform,
nvl(log_data.original_channel,'all') original_channel,
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
nvl(t1.geo_country,'NALL') region_code,
nvl(t1.platform,'NA') platform,
t1.pt,
nvl(voc.original_channel, 'unknown') AS original_channel,
CASE when t1.event_name ='page_view' and t1.page_code='homepage' THEN t1.domain_userId end homepage_domain_userId,
CASE when t1.event_name ='page_view' THEN t1.domain_userId end activate_domain_userId,
CASE when t1.event_name ='page_view' and  t1.page_code='cart' THEN t1.domain_userId end cart_domain_userId,
CASE when t1.event_name ='page_view' and  t1.page_code='checkout' THEN t1.domain_userId end check_out_domain_userId,
CASE when t1.event_name ='page_view' and  t1.page_code in ('product', 'product_detail') THEN t1.domain_userId end product_detail_domain_userId,
CASE when t1.event_name ='data' and t1.page_code in ('product', 'product_detail') and t1.element_name = 'pdAddToCartSuccess' THEN t1.domain_userId end add_cart_success_domain_userId,
CASE when t1.event_name ='common_click' and t1.page_code='checkout' and t1.element_name = 'continue_checkout' THEN t1.domain_userId end continue_checkout_domain_userId
from
(
select pt,datasource,domain_userId,platform,buyer_id,event_name,geo_country,os_type,page_code,device_id,referrer,view_type,NULL element_name,NULL list_name from dwd.dwd_vova_log_page_view where pt='${cur_date}' and datasource IN ('vova') and platform in ('pc', 'web')
union all
select pt,datasource,domain_userId,platform,buyer_id,event_name,geo_country,os_type,page_code,device_id,referrer,NULL view_type,element_name,NULL list_name from dwd.dwd_vova_log_data where pt='${cur_date}' and datasource IN ('vova') and platform in ('pc', 'web')
union all
select pt,datasource,domain_userId,platform,buyer_id,event_name,geo_country,os_type,page_code,device_id,referrer,NULL view_type,element_name,NULL list_name from dwd.dwd_vova_log_common_click where pt='${cur_date}' and datasource IN ('vova') and platform in ('pc', 'web')
) t1
left join dwd.dwd_vova_fact_original_channel voc on voc.domain_userid = t1.domain_userid
) log_data
group by cube(log_data.datasource, log_data.region_code, log_data.platform, log_data.original_channel, log_data.pt)
UNION ALL
select
nvl(nvl(fp.datasource,'NA'),'all') AS datasource,
nvl(date(fp.pay_time),'all') AS event_date,
nvl(nvl(fp.region_code,'NALL') ,'all') region_code,
'all' AS platform,
nvl(nvl(voc.original_channel, 'unknown'), 'all') AS original_channel,
0 AS dau,
0 as home_page_uv,
0 as cart_uv,
0 as checkout_uv,
0 as product_detail_uv,
0 as checkout_click_success_uv,
0 as continue_checkout_uv,
sum(fp.shop_price * fp.goods_number + fp.shipping_fee) AS gmv,
count(DISTINCT fp.buyer_id) AS paid_uv,
count(DISTINCT fp.order_id) AS paid_order_cnt,
count(DISTINCT if(pre_order.min_order_id is not null, fp.order_id, null)) AS first_order_cnt
from
dwd.dwd_vova_fact_pay fp
LEFT JOIN dwd.dwd_vova_fact_original_channel voc on voc.domain_userid = fp.device_id AND voc.datasource = fp.datasource
LEFT JOIN (
SELECT fp.buyer_id,
       min(fp.order_id) AS min_order_id,
       fp.datasource
FROM dwd.dwd_vova_fact_pay fp
group by fp.buyer_id, fp.datasource
) pre_order on pre_order.min_order_id = fp.order_id AND fp.datasource = pre_order.datasource
WHERE date(fp.pay_time) = '${cur_date}'
AND fp.datasource = 'vova'
AND fp.from_domain not like '%api%'
GROUP BY CUBE (date(fp.pay_time), nvl(fp.region_code,'NALL'), nvl(fp.datasource,'NA'), nvl(voc.original_channel, 'unknown'))
) fin
group by fin.event_date, fin.region_code, fin.platform, fin.original_channel, fin.datasource
having event_date != 'all'
;









INSERT OVERWRITE TABLE dwb.dwb_vova_web_main_goods PARTITION (pt)
SELECT
/*+ REPARTITION(1) */
    fin.pt AS event_date,
    fin.datasource,
    fin.platform,
    fin.region_code,
    fin.original_channel,
    nvl(fin.impressions, 0)    AS impressions,
    nvl(fin.impressions_uv, 0) AS impressions_uv,
    nvl(fin.clicks, 0)         AS clicks,
    nvl(fin.clicks_uv, 0)      AS clicks_uv,
    fin.pt
FROM (
         SELECT final.pt,
                final.datasource,
                final.platform,
                final.region_code,
                final.original_channel,
                sum(impressions) AS impressions,
                sum(impressions_uv) AS impressions_uv,
                sum(clicks)      AS clicks,
                sum(clicks_uv)      AS clicks_uv
         FROM (
                  SELECT nvl(log.pt, 'all') AS pt,
                         nvl(log.datasource, 'all') AS datasource,
                         nvl(log.platform, 'all') AS platform,
                         nvl(nvl(log.geo_country,'NALL'),'all') region_code,
                         nvl(nvl(voc.original_channel, 'unknown'),'all') original_channel,
                         count(*) AS impressions,
                         count(DISTINCT log.domain_userid) AS impressions_uv,
                         0        AS clicks,
                         0        AS clicks_uv
                  FROM dwd.dwd_vova_log_goods_impression log
                   LEFT JOIN dwd.dwd_vova_fact_original_channel voc on voc.domain_userid = log.domain_userid AND voc.datasource = log.datasource
                  WHERE log.pt = '${cur_date}'
                    AND log.platform IN ('pc', 'web')
                    AND log.datasource = 'vova'
                  GROUP BY CUBE (log.pt, log.platform, nvl(log.geo_country,'NALL'), nvl(voc.original_channel, 'unknown'), log.datasource)
                  UNION ALL
                  SELECT nvl(log.pt, 'all') AS pt,
                         nvl(log.datasource, 'all') AS datasource,
                         nvl(log.platform, 'all') AS platform,
                         nvl(nvl(log.geo_country,'NALL'),'all') region_code,
                         nvl(nvl(voc.original_channel, 'unknown'),'all') original_channel,
                         0        AS impressions,
                         0        AS impressions_uv,
                         count(*) AS clicks,
                         count(DISTINCT log.domain_userid) AS clicks_uv
                  FROM dwd.dwd_vova_log_goods_click log
                   LEFT JOIN dwd.dwd_vova_fact_original_channel voc on voc.domain_userid = log.domain_userid AND voc.datasource = log.datasource
                  WHERE log.pt = '${cur_date}'
                    AND log.platform IN ('pc', 'web')
                    AND log.datasource = 'vova'
                  GROUP BY CUBE (log.pt, log.platform, nvl(log.geo_country,'NALL'), nvl(voc.original_channel, 'unknown'), log.datasource)
              ) final
         GROUP BY final.pt, final.platform, final.region_code, final.original_channel, final.datasource
         HAVING pt != 'all'
     ) fin
;
"

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=dwb_vova_web_main_process" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi


