#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
-- req7343
WITH ads_activity_clearance_sale_tmp_gooods AS (
SELECT
    cb.goods_id,
    cb.first_cat_id,
    cb.second_cat_id,
    cb.region_id,
    cb.expre_cnt,
    cb.clk_cnt,
    cb.ord_cnt,
    cb.gmv,
    cb.expre_uv,
    cb.click_uv,
    cb.sales_vol
FROM
    dwd.dwd_vova_activity_goods_ctry_behave cb
WHERE
    cb.pt = '${pre_date}'
    AND cb.is_brand = 0
    AND cb.expre_cnt >= 500
    AND cb.region_id IN ( 0, 3858, 4003, 4017, 4056, 4143 ))


INSERT OVERWRITE table ads.ads_vova_activity_clearance_sale partition(pt='${pre_date}')
SELECT
goods_id,
region_id,
biz_type,
rp_type,
first_cat_id,
second_cat_id,
rank
FROM
    (
SELECT
    goods_id,
    region_id,
    'qingcang-temai' AS biz_type,
    3 AS rp_type,
    first_cat_id,
    nvl ( second_cat_id, 0 ) AS second_cat_id,
    row_number ( ) over ( PARTITION BY first_cat_id, region_id ORDER BY gmv / click_uv * clk_cnt / expre_cnt DESC ) AS rank
FROM
    ads_activity_clearance_sale_tmp_gooods tmp1
WHERE
    ( region_id in (3858,4056) AND gmv / click_uv * clk_cnt / expre_cnt * 10000 > 50 )
    OR ( region_id in (4003,4017,4143) AND gmv / click_uv * clk_cnt / expre_cnt * 10000 > 60 )
    OR ( region_id = 0 AND gmv / click_uv * clk_cnt / expre_cnt * 10000 > 80 )
    ) tmp2
WHERE
    tmp2.rank <= 300;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_activity_clearance_sale" \
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