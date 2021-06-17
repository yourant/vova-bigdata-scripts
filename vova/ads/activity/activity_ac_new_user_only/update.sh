#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
INSERT overwrite TABLE ads.ads_vova_activity_ac_new_user_only_goods PARTITION (pt = '${pre_date}')
SELECT
/*+ REPARTITION(1) */
t2.goods_id,
t2.region_id,
'ac-new-user-only-brand' AS biz_type,
3 AS rp_type,
nvl(dg.first_cat_id, 0) AS first_cat_id,
nvl(dg.second_cat_id, 0) AS second_cat_id,
t2.rank
from
(
select
goods_id,
region_id,
row_number() over(PARTITION BY region_id ORDER BY nvl(sales_order / impressions, 0) DESC) rank
from
(
SELECT
dg.goods_id,
nvl(r.region_id, 0) AS region_id,
sum(gp.sales_order ) as sales_order,
sum(gp.impressions ) as impressions,
sum(gp.clicks ) as clicks
from
ads.ads_vova_goods_performance gp
inner join dim.dim_vova_goods dg on dg.goods_id = gp.goods_id
LEFT JOIN ods_vova_vts.ods_vova_region r on r.region_code = gp.region_code AND r.region_type = 0 AND r.region_display = 1
where gp.pt = '${pre_date}'
AND gp.datasource IN ('airyclub', 'app_group')
AND gp.platform = 'mob'
AND gp.region_code in ('all', 'FR', 'DE', 'IT', 'ES', 'GB')
AND gp.ctr > 0
AND dg.shop_price + dg.shipping_fee <= 4.8
AND dg.brand_id > 0
group by dg.goods_id, nvl(r.region_id, 0)
) t1
) t2
inner join dim.dim_vova_goods dg on dg.goods_id = t2.goods_id
WHERE t2.rank <= 15000

UNION ALL

SELECT
/*+ REPARTITION(1) */
t2.goods_id,
t2.region_id,
'ac-new-user-only-all' AS biz_type,
3 AS rp_type,
nvl(dg.first_cat_id, 0) AS first_cat_id,
nvl(dg.second_cat_id, 0) AS second_cat_id,
t2.rank
from
(
select
goods_id,
region_id,
row_number() over(PARTITION BY region_id ORDER BY nvl(sales_order / impressions, 0) DESC) rank
from
(
SELECT
dg.goods_id,
nvl(r.region_id, 0) AS region_id,
sum(gp.sales_order ) as sales_order,
sum(gp.impressions ) as impressions,
sum(gp.clicks ) as clicks
from
ads.ads_vova_goods_performance gp
inner join dim.dim_vova_goods dg on dg.goods_id = gp.goods_id
LEFT JOIN ods_vova_vts.ods_vova_region r on r.region_code = gp.region_code AND r.region_type = 0 AND r.region_display = 1
where gp.pt = '${pre_date}'
AND gp.datasource IN ('airyclub', 'app_group')
AND gp.platform = 'mob'
AND gp.region_code in ('all', 'FR', 'DE', 'IT', 'ES', 'GB')
AND gp.ctr > 0
AND dg.shop_price + dg.shipping_fee <= 4.8
group by dg.goods_id, nvl(r.region_id, 0)
) t1
) t2
inner join dim.dim_vova_goods dg on dg.goods_id = t2.goods_id
WHERE t2.rank <= 15000
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_activity_ac_new_user_only_goods" \
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
if [ $? -ne 0 ]; then
  exit 1
fi
