#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-0 day" +%Y-%m-%d`
fi
###逻辑sql
#dependence
#ods_vova_goods_h
#ods_vova_virtual_goods_h
#ods_vova_order_info_h
#ods_vova_order_goods_h
#ods_vova_vbai.ods_vova_images_vector
#dim_vova_category
#dwd_vova_log_goods_impression_arc
#dwd_vova_log_goods_click_arc
#ads_vova_six_rank_mct
sql="
DROP TABLE IF EXISTS tmp.tmp_ads_vova_six_mct_flow_support_goods;
CREATE TABLE tmp.tmp_ads_vova_six_mct_flow_support_goods
select
/*+ REPARTITION(1) */
goods_id,
virtual_goods_id,
first_cat_id,
mct_id,
mct_name
from
(
select
goods_id,
virtual_goods_id,
first_cat_id,
mct_id,
mct_name,
row_number() over(partition by goods_id order by mct_id desc) rank
from
(
SELECT
    dg.goods_id,
    dg.virtual_goods_id,
    dg.first_cat_id,
    dg.mct_id,
    six.mct_name
FROM ads.ads_vova_six_rank_mct six
         INNER JOIN
     (
         SELECT g.goods_id,
                vg.virtual_goods_id,
                dc.first_cat_id,
                g.merchant_id AS mct_id
         FROM ods_vova_vts.ods_vova_goods_h g
                  INNER JOIN ods_vova_vts.ods_vova_virtual_goods_h vg ON vg.goods_id = g.goods_id
                  INNER JOIN dim.dim_vova_category dc ON dc.cat_id = g.cat_id
     ) dg ON dg.first_cat_id = six.first_cat_id AND dg.mct_id = six.mct_id
union all
select
goods_id,
virtual_goods_id,
first_cat_id,
mct_id,
mct_name
from dim.dim_vova_virtual_six_mct_goods
) t
) t where rank =1;

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
list_type,
recall_pool,
get_rp_name(recall_pool) AS recall_pool_name,
pt
from
(
select
dg.goods_id,
'goods_impression' as original_name,
log.device_id,
log.collector_ts,
log.page_code,
log.list_type,
log.recall_pool,
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
'goods_impression' as original_name,
log.device_id,
log.collector_ts,
log.page_code,
log.list_type,
log.recall_pool,
log.pt
FROM dwd.dwd_vova_log_impressions_arc log
         INNER JOIN tmp.tmp_ads_vova_six_mct_flow_support_goods dg ON log.element_id = dg.virtual_goods_id
WHERE log.datasource = 'vova'
  AND log.platform = 'mob'
  AND log.pt >= date_sub('${cur_date}', 1)
  AND log.pt <= '${cur_date}'
  AND log.element_id is not null

UNION ALL

select
dg.goods_id,
'goods_click' as original_name,
log.device_id,
log.collector_ts,
log.page_code,
log.list_type,
log.recall_pool,
log.pt
FROM dwd.dwd_vova_log_goods_click_arc log
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
log.list_type,
log.recall_pool,
log.pt
FROM dwd.dwd_vova_log_click_arc log
         INNER JOIN tmp.tmp_ads_vova_six_mct_flow_support_goods dg ON log.element_id = dg.virtual_goods_id
WHERE log.datasource = 'vova'
  AND log.platform = 'mob'
  AND log.pt >= date_sub('${cur_date}', 1)
  AND log.pt <= '${cur_date}'
  AND log.event_type='goods'
  AND log.element_id is not null
) t1
;

DROP TABLE IF EXISTS tmp.tmp_ads_vova_six_mct_flow_support_goods_min_collector;
CREATE TABLE tmp.tmp_ads_vova_six_mct_flow_support_goods_min_collector
SELECT /*+ REPARTITION(1) */
       log.goods_id,
       min(collector_ts) AS min_collector_ts
FROM ads.ads_vova_six_mct_flow_support_collector_data log
WHERE log.original_name = 'goods_impression'
AND log.recall_pool_name LIKE '%59%'
group by log.goods_id
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
WHERE log.recall_pool_name LIKE '%59%'
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
         INNER JOIN tmp.tmp_ads_vova_six_mct_flow_support_goods_min_collector min ON min.goods_id = og.goods_id
WHERE oi.pay_status >= 1
  AND oi.from_domain LIKE '%api%'
  AND oi.project_name = 'vova'
  AND oi.email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
  AND oi.parent_order_id = 0
  AND oi.order_time >= min.min_collector_ts
group by og.goods_id
) t1
group by goods_id

;

DROP TABLE IF EXISTS tmp.tmp_ads_vova_six_mct_flow_support_page_goods_1d;
CREATE TABLE tmp.tmp_ads_vova_six_mct_flow_support_page_goods_1d
select
/*+ REPARTITION(1) */
log.goods_id,
case
when page_code in ('homepage','product_list') and  list_type in ('/product_list_popular','/product_list') then 'product_list'
when page_code ='product_detail' and list_type ='/detail_also_like' then 'product_detail'
else concat(page_code, '_new') end as page_code,
count(*) AS impressions
from
ads.ads_vova_six_mct_flow_support_collector_data log
where log.pt = '${cur_date}'
AND log.original_name = 'goods_impression'
AND log.recall_pool_name LIKE '%59%'
group by log.goods_id,case
when page_code in ('homepage','product_list') and  list_type in ('/product_list_popular','/product_list') then 'product_list'
when page_code ='product_detail' and list_type ='/detail_also_like' then 'product_detail'
else concat(page_code, '_new') end
;

DROP TABLE IF EXISTS tmp.tmp_ads_vova_six_mct_flow_support_page_mct_1d;
CREATE TABLE tmp.tmp_ads_vova_six_mct_flow_support_page_mct_1d
select
/*+ REPARTITION(1) */
dg.mct_id,
case
when page_code in ('homepage','product_list') and  list_type in ('/product_list_popular','/product_list') then 'product_list'
when page_code ='product_detail' and list_type ='/detail_also_like' then 'product_detail'
else concat(page_code, '_new') end as page_code,
count(*) AS impressions
from
ads.ads_vova_six_mct_flow_support_collector_data log
INNER JOIN tmp.tmp_ads_vova_six_mct_flow_support_goods dg ON log.goods_id = dg.goods_id
where log.pt = '${cur_date}'
AND log.original_name = 'goods_impression'
AND log.recall_pool_name LIKE '%59%'
group by dg.mct_id,
case
when page_code in ('homepage','product_list') and  list_type in ('/product_list_popular','/product_list') then 'product_list'
when page_code ='product_detail' and list_type ='/detail_also_like' then 'product_detail'
else concat(page_code, '_new') end
;


INSERT OVERWRITE TABLE ads.ads_vova_six_mct_flow_support_goods_page_process PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
dg.goods_id,
page_code_list.page_code,
nvl(his.impressions, 0) AS his_impressions,
nvl(t1.impressions, 0) AS t1_impressions,
nvl(t2.impressions, 0) AS t2_impressions,
nvl(t3.impressions, 0) AS t3_impressions,
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

INSERT OVERWRITE TABLE ads.ads_vova_six_mct_flow_support_goods_behave_h PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
dg.goods_id,
nvl(his.impressions, 0) AS his_impressions,
nvl(t1.impressions, 0) AS goods_page_impressions,
nvl(t2.impressions, 0) AS mct_page_impressions,
nvl(t3.impressions, 0) AS mct_impressions,
case
when his.impressions >= 30000 THEN 'a'
when his.impressions >= 20000 AND gcr < 60 THEN 'g'
when his.impressions >= 10000 AND gcr < 60 THEN 'f'
when his.impressions >= 5000 AND sales_order < 1 THEN 'e'
when his.impressions >= 2000 AND ctr < 0.014 THEN 'd'
when t1.impressions >= 5000 THEN 'a'
when t2.impressions >= 50000 THEN 'b'
when t3.impressions >= 100000 THEN 'b'
else 'normal' end AS block_reason
from
tmp.tmp_ads_vova_six_mct_flow_support_goods dg
LEFT JOIN ads.ads_vova_six_mct_flow_support_goods_his his ON his.goods_id = dg.goods_id AND his.pt = '${cur_date}'
LEFT JOIN (
SELECT
goods_id,
max(impressions) AS impressions
FROM
tmp.tmp_ads_vova_six_mct_flow_support_page_goods_1d
group by goods_id
) t1 on t1.goods_id = dg.goods_id
LEFT JOIN (
SELECT
mct_id,
max(impressions) AS impressions
FROM
tmp.tmp_ads_vova_six_mct_flow_support_page_mct_1d
group by mct_id
) t2 on t2.mct_id = dg.mct_id
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
/*+ REPARTITION(1) */
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
