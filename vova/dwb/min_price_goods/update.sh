#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "$cur_date"
##dependence
#dwd.dwd_vova_log_goods_impression
#dwd.dwd_vova_fact_pay
#dim.dim_vova_goods
#ads.ads_vova_min_price_goods_h
#ads.ads_vova_mct_rank

sql="
INSERT overwrite TABLE tmp.tmp_dwb_vova_min_goods_group
SELECT
/*+ REPARTITION(1) */
goods_id,
min_price_goods_id
FROM ads.ads_vova_min_price_goods_h
WHERE pt = '${cur_date}'
  AND strategy = 'a'
UNION
SELECT
dg.goods_id,
dg.goods_id AS min_price_goods_id
FROM
dim.dim_vova_goods dg
LEFT JOIN (
SELECT
DISTINCT goods_id
FROM ads.ads_vova_min_price_goods_h
WHERE pt = '${cur_date}'
  AND strategy = 'a'
) t1 on t1.goods_id = dg.goods_id
WHERE t1.goods_id is null
;

INSERT overwrite TABLE dwb.dwb_vova_min_price_goods_summary PARTITION (pt='${cur_date}')
SELECT
/*+ REPARTITION(1) */
tot_impression.event_date,
tot_impression.tot_impression,
min_impression.min_impression,
nvl(min_impression.min_impression / tot_impression.tot_impression, 0) AS min_exopsure_rate
FROM
(
SELECT
'${cur_date}' AS event_date,
sum(impression_pv) AS tot_impression
FROM (
         SELECT log.virtual_goods_id,
                count(*) AS impression_pv
         FROM dwd.dwd_vova_log_goods_impression log
         WHERE log.pt = '${cur_date}'
          AND !(
          (log.page_code = 'search_result'
          AND log.list_type IN ('/search_result_recommend', '/search_result', '/search_result_sold', '/search_result_price_desc', '/search_result_price_asc', '/search_result_newarrival')
          )
          OR
          (log.page_code = 'product_list'
          AND log.list_type IN ('/product_list_sold','/product_list_newarrival','/product_list_price_asc','/product_list_price_desc')
          )
          OR
          (log.page_code = 'flashsale'
          AND log.list_type IN ('/onsale', 'upcoming'))
          )
          AND log.platform = 'mob'
         GROUP BY log.virtual_goods_id
 ) t1
) tot_impression
LEFT JOIN
(
SELECT
'${cur_date}' AS event_date,
sum(impression_pv) AS min_impression
FROM (
         SELECT DISTINCT min_price_goods_id
         FROM tmp.tmp_dwb_vova_min_goods_group
     ) min_price_goods
         INNER JOIN dim.dim_vova_goods dg ON dg.goods_id = min_price_goods.min_price_goods_id
         INNER JOIN
     (
         SELECT log.virtual_goods_id,
                count(*) AS impression_pv
         FROM dwd.dwd_vova_log_goods_impression log
         WHERE log.pt = '${cur_date}'
          AND !(
          (log.page_code = 'search_result'
          AND log.list_type IN ('/search_result_recommend', '/search_result', '/search_result_sold', '/search_result_price_desc', '/search_result_price_asc', '/search_result_newarrival')
          )
          OR
          (log.page_code = 'product_list'
          AND log.list_type IN ('/product_list_sold','/product_list_newarrival','/product_list_price_asc','/product_list_price_desc')
          )
          OR
          (log.page_code = 'flashsale'
          AND log.list_type IN ('/onsale', 'upcoming'))
          )
          AND log.platform = 'mob'
         GROUP BY log.virtual_goods_id
     ) impression_data ON impression_data.virtual_goods_id = dg.virtual_goods_id
) min_impression ON tot_impression.event_date = min_impression.event_date
;

--detail
INSERT overwrite TABLE tmp.tmp_dwb_vova_min_price_goods_detail
select
/*+ REPARTITION(1) */
tot_impression.event_date,
tot_impression.min_price_goods_id,
tot_impression.tot_impression,
min_impression.min_impression,
nvl(min_impression.min_impression / tot_impression.tot_impression, 0) AS min_exopsure_rate,
CASE
WHEN
nvl(min_impression.min_impression / tot_impression.tot_impression, 0) >= 0 AND nvl(min_impression.min_impression / tot_impression.tot_impression, 0) <= 0.1
THEN '[0,10%]'
WHEN
nvl(min_impression.min_impression / tot_impression.tot_impression, 0) > 0.1 AND nvl(min_impression.min_impression / tot_impression.tot_impression, 0) <= 0.2
THEN '(10%,20%]'
WHEN
nvl(min_impression.min_impression / tot_impression.tot_impression, 0) > 0.2 AND nvl(min_impression.min_impression / tot_impression.tot_impression, 0) <= 0.3
THEN '(20%,30%]'
ELSE '(30%,100%]' END AS impression_rate_range,
dg.shop_price + dg.shipping_fee AS shop_price_amount,
tot_impression.tot_gmv,
min_impression.min_gmv,
nvl(min_impression.min_gmv / tot_impression.tot_gmv, 0) AS min_gmv_rate,
dg.first_cat_name,
amp.rank
from
(
SELECT
'${cur_date}' AS event_date,
min_price_goods.min_price_goods_id,
sum(impression_data.impression_pv) AS tot_impression,
sum(pay_data.gmv) AS tot_gmv
FROM (
         SELECT goods_id,min_price_goods_id
         FROM tmp.tmp_dwb_vova_min_goods_group
           GROUP BY goods_id,min_price_goods_id
     ) min_price_goods
         INNER JOIN dim.dim_vova_goods dg ON dg.goods_id = min_price_goods.goods_id
         INNER JOIN
     (
         SELECT log.virtual_goods_id,
                count(*) AS impression_pv
         FROM dwd.dwd_vova_log_goods_impression log
         WHERE log.pt = '${cur_date}'
          AND !(
          (log.page_code = 'search_result'
          AND log.list_type IN ('/search_result_recommend', '/search_result', '/search_result_sold', '/search_result_price_desc', '/search_result_price_asc', '/search_result_newarrival')
          )
          OR
          (log.page_code = 'product_list'
          AND log.list_type IN ('/product_list_sold','/product_list_newarrival','/product_list_price_asc','/product_list_price_desc')
          )
          OR
          (log.page_code = 'flashsale'
          AND log.list_type IN ('/onsale', 'upcoming'))
          )
          AND log.platform = 'mob'
         GROUP BY log.virtual_goods_id
     ) impression_data ON impression_data.virtual_goods_id = dg.virtual_goods_id
     LEFT JOIN
     (
       SELECT
       fp.goods_id,
       sum(fp.shop_price * fp.goods_number + fp.shipping_fee) AS gmv
       FROM
       dwd.dwd_vova_fact_pay fp
       WHERE date(fp.pay_time) = '${cur_date}'
       GROUP BY fp.goods_id
     ) pay_data ON pay_data.goods_id = dg.goods_id
GROUP BY min_price_goods.min_price_goods_id
) tot_impression
LEFT JOIN
(
SELECT
'${cur_date}' AS event_date,
min_price_goods.min_price_goods_id,
impression_pv AS min_impression,
gmv AS min_gmv
FROM (
         SELECT distinct min_price_goods_id
         FROM tmp.tmp_dwb_vova_min_goods_group
           GROUP BY min_price_goods_id
     ) min_price_goods
         INNER JOIN dim.dim_vova_goods dg ON dg.goods_id = min_price_goods.min_price_goods_id
         INNER JOIN
     (
         SELECT log.virtual_goods_id,
                count(*) AS impression_pv
         FROM dwd.dwd_vova_log_goods_impression log
         WHERE log.pt = '${cur_date}'
          AND !(
          (log.page_code = 'search_result'
          AND log.list_type IN ('/search_result_recommend', '/search_result', '/search_result_sold', '/search_result_price_desc', '/search_result_price_asc', '/search_result_newarrival')
          )
          OR
          (log.page_code = 'product_list'
          AND log.list_type IN ('/product_list_sold','/product_list_newarrival','/product_list_price_asc','/product_list_price_desc')
          )
          OR
          (log.page_code = 'flashsale'
          AND log.list_type IN ('/onsale', 'upcoming'))
          )
          AND log.platform = 'mob'
         GROUP BY log.virtual_goods_id
     ) impression_data ON impression_data.virtual_goods_id = dg.virtual_goods_id
     LEFT JOIN
     (
       SELECT
       fp.goods_id,
       sum(fp.shop_price * fp.goods_number + fp.shipping_fee) AS gmv
       FROM
       dwd.dwd_vova_fact_pay fp
       WHERE date(fp.pay_time) = '${cur_date}'
       GROUP BY fp.goods_id
     ) pay_data ON pay_data.goods_id = dg.goods_id
) min_impression  ON tot_impression.min_price_goods_id = min_impression.min_price_goods_id
AND tot_impression.event_date = min_impression.event_date
LEFT JOIN dim.dim_vova_goods dg on dg.goods_id = tot_impression.min_price_goods_id
LEFT JOIN
(
SELECT
amp.mct_id,
amp.first_cat_id,
amp.rank
FROM ads.ads_vova_mct_rank amp
WHERE amp.pt = '${cur_date}'
) amp ON amp.mct_id = dg.mct_id
AND amp.first_cat_id = dg.first_cat_id
;


INSERT overwrite TABLE dwb.dwb_vova_min_price_goods_detail PARTITION (pt='${cur_date}')
select
/*+ REPARTITION(1) */
event_date,
min_price_goods_id,
first_cat_name,
shop_price_amount,
rank AS mct_rank,
min_impression,
min_exopsure_rate,
impression_rate_range,
min_gmv_rate
FROM
tmp.tmp_dwb_vova_min_price_goods_detail
WHERE event_date = '${cur_date}'
AND cast(min_exopsure_rate as double) <= 0.3

"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=20" --conf "spark.app.name=dwb_vova_min_price_goods" -e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi