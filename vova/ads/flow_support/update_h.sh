#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
#dependence
#ads_vova_six_rank_mct
#ods_vova_vbai.ods_vova_images_vector
sql="
DROP TABLE IF EXISTS tmp.tmp_ads_vova_six_mct_flow_support_goods;
CREATE TABLE tmp.tmp_ads_vova_six_mct_flow_support_goods
SELECT /*+ REPARTITION(1) */
       dg.goods_id,
       dg.virtual_goods_id,
       dg.first_cat_id,
       dg.mct_id,
       six.mct_name
FROM ads.ads_vova_six_rank_mct six
         INNER JOIN dim.dim_vova_goods dg ON dg.first_cat_id = six.first_cat_id AND dg.mct_id = six.mct_id
;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE ads.ads_vova_six_mct_flow_support_collector_data PARTITION (pt)
select
/*+ REPARTITION(1) */
goods_id,
device_id,
original_name,
collector_ts,
page_code,
pt
from
(
select
dg.goods_id,
'goods_impression' as original_name,
log.device_id,
log.collector_ts,
log.page_code,
log.pt
FROM dwd.dwd_vova_log_goods_impression_arc log
         INNER JOIN tmp.tmp_ads_vova_six_mct_flow_support_goods dg ON log.virtual_goods_id = dg.virtual_goods_id
WHERE log.datasource = 'vova'
  AND log.platform = 'mob'
  AND log.pt >= date_sub('${cur_date}', 1)
  AND log.pt <= '${cur_date}'

UNION ALL

select
dg.goods_id,
'goods_click' as original_name,
log.device_id,
log.collector_ts,
log.page_code,
log.pt
FROM dwd.dwd_vova_log_goods_click_arc log
         INNER JOIN tmp.tmp_ads_vova_six_mct_flow_support_goods dg ON log.virtual_goods_id = dg.virtual_goods_id
WHERE log.datasource = 'vova'
  AND log.platform = 'mob'
  AND log.pt >= date_sub('${cur_date}', 1)
  AND log.pt <= '${cur_date}'
) t1
;

INSERT OVERWRITE TABLE ads.ads_vova_six_mct_flow_support_goods_his PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
goods_id,
sum(impressions) AS impressions,
sum(clicks) AS clicks,
sum(clicks_uv) AS clicks_uv,
sum(gmv) AS gmv,
sum(sales_order) AS sales_order,
nvl(sum(clicks) / sum(impressions), 0) AS ctr,
nvl(sum(gmv) / sum(clicks_uv) * sum(clicks) / sum(impressions) * 10000, 0) AS gcr
from
(
select
log.goods_id,
sum(if(log.original_name = 'goods_impression', 1, 0)) AS impressions,
sum(if(log.original_name = 'goods_click', 1, 0)) AS clicks,
count(DISTINCT if(log.original_name = 'goods_click', log.device_id, null)) AS clicks_uv,
0 AS gmv,
0 AS sales_order
from
ads.ads_vova_six_mct_flow_support_collector_data log
group by log.goods_id

UNION ALL

SELECT
og.goods_id,
0 AS impressions,
0 AS clicks,
0 AS clicks_uv,
sum(og.goods_number * og.shop_price + og.shipping_fee) as gmv,
COUNT(DISTINCT oi.order_id) AS sales_order
FROM ods_vova_vts.ods_vova_order_info_h oi
         INNER JOIN ods_vova_vts.ods_vova_order_goods_h og ON oi.order_id = og.order_id
         INNER JOIN tmp.tmp_ads_vova_six_mct_flow_support_goods dg ON og.goods_id = dg.goods_id
WHERE oi.pay_status >= 1
  AND oi.from_domain LIKE '%api%'
  AND oi.project_name = 'vova'
  AND oi.email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
  AND oi.parent_order_id = 0
  AND date(oi.pay_time) >= '2021-03-26'
group by og.goods_id
) t1
group by goods_id

;

DROP TABLE IF EXISTS tmp.tmp_ads_vova_six_mct_flow_support_page_goods_1d;
CREATE TABLE tmp.tmp_ads_vova_six_mct_flow_support_page_goods_1d
select
/*+ REPARTITION(1) */
log.goods_id,
log.page_code,
count(*) AS impressions
from
ads.ads_vova_six_mct_flow_support_collector_data log
where log.pt = '${cur_date}'
AND log.original_name = 'goods_impression'
AND log.page_code IN ('product_detail', 'product_list')
group by log.goods_id,log.page_code
;

DROP TABLE IF EXISTS tmp.tmp_ads_vova_six_mct_flow_support_page_mct_1d;
CREATE TABLE tmp.tmp_ads_vova_six_mct_flow_support_page_mct_1d
select
/*+ REPARTITION(1) */
dg.mct_id,
log.page_code,
count(*) AS impressions
from
ads.ads_vova_six_mct_flow_support_collector_data log
INNER JOIN tmp.tmp_ads_vova_six_mct_flow_support_goods dg ON log.goods_id = dg.goods_id
where log.pt = '${cur_date}'
AND log.original_name = 'goods_impression'
group by dg.mct_id,log.page_code
;


INSERT OVERWRITE TABLE ads.ads_vova_six_mct_flow_support_goods_page_process PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
dg.goods_id,
page_code_list.page_code,
his.impressions AS his_impressions,
t1.impressions AS t1_impressions,
t2.impressions AS t2_impressions,
t3.impressions AS t3_impressions,
case
when his.impressions >= 30000 THEN 1
when his.impressions >= 20000 AND gcr < 60 THEN 1
when his.impressions >= 10000 AND gcr < 60 THEN 1
when his.impressions >= 5000 AND sales_order < 1 THEN 1
when his.impressions >= 2000 AND ctr < 0.014 THEN 1
when t1.impressions >= 5000 THEN 1
when t2.impressions >= 50000 THEN 1
when t3.impressions >= 100000 THEN 1
else 0 end AS is_delete
from
tmp.tmp_ads_vova_six_mct_flow_support_goods dg
CROSS JOIN
(
    select page_code
from (select 'product_detail,product_list' AS page_code_list) t
lateral view explode(split(page_code_list,',')) num as page_code
) page_code_list on 1=1
LEFT JOIN ads.ads_vova_six_mct_flow_support_goods_his his ON his.goods_id = dg.goods_id AND his.pt = '${cur_date}'
LEFT JOIN (
SELECT
goods_id,
page_code,
impressions
FROM
tmp.tmp_ads_vova_six_mct_flow_support_page_goods_1d
) t1 on t1.goods_id = dg.goods_id AND t1.page_code = page_code_list.page_code
LEFT JOIN (
SELECT
mct_id,
page_code,
impressions
FROM
tmp.tmp_ads_vova_six_mct_flow_support_page_mct_1d
) t2 on t2.mct_id = dg.mct_id AND t2.page_code = page_code_list.page_code
LEFT JOIN (
SELECT
mct_id,
sum(impressions) AS impressions
FROM
tmp.tmp_ads_vova_six_mct_flow_support_page_mct_1d
group by mct_id
) t3 on t3.mct_id = dg.mct_id
;


INSERT OVERWRITE TABLE ads.ads_vova_six_mct_goods_flow_support_h PARTITION (pt = '${cur_date}')
select
goods_id,
page_code
from
ads.ads_vova_six_mct_flow_support_goods_page_process
where pt = '${cur_date}'
  AND is_delete = 0
;




"


#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=10" \
--conf "spark.dynamicAllocation.maxExecutors=50" \
--conf "spark.app.name=ads_vova_six_mct_goods_flow_support_h" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
