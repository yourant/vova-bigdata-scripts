#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
WITH tmp_all AS (
SELECT
  cb.goods_id,
  cb.region_id,
  cb.expre_cnt,
  cb.clk_cnt,
  cb.ord_cnt,
  cb.first_cat_id,
  nvl(cb.second_cat_id, 0) second_cat_id,
  cb.gmv,
  cb.click_uv
FROM
  dwd.dwd_vova_activity_goods_ctry_behave cb
WHERE cb.pt = '${pre_date}'
  AND cb.is_brand = 0
  AND cb.region_id != 0
  AND expre_cnt >= 500
  AND expre_cnt <= 200000
  AND clk_cnt / expre_cnt >= 0.02
),

-- 非brand非女装
tmp_no_brand_no_women_clothes (
SELECT
  4 AS event_type,
  tmp_all.region_id,
  first_cat_id,
  second_cat_id,
  tmp_all.goods_id,
  row_number() over (PARTITION BY tmp_all.region_id ORDER BY tmp_all.ord_cnt / tmp_all.expre_cnt) rk
FROM
  tmp_all
WHERE first_cat_id != 194
  AND tmp_all.ord_cnt/tmp_all.click_uv >= 0.04
),

--非 brand女装
tmp_no_brand_women_clothes (
SELECT
  57 AS event_type,
  tmp_all.region_id,
  tmp_all.first_cat_id,
  tmp_all.second_cat_id,
  tmp_all.goods_id,
  row_number() over (PARTITION BY tmp_all.region_id ORDER BY tmp_all.ord_cnt / tmp_all.expre_cnt) rk
FROM
  tmp_all
WHERE first_cat_id = 194
  AND (
    (tmp_all.region_id in (4003,4017,4056) AND tmp_all.ord_cnt/tmp_all.click_uv >= 0.04)
    OR (tmp_all.region_id IN (4143, 3858) AND tmp_all.ord_cnt/tmp_all.click_uv >= 0.03)
    OR tmp_all.region_id NOT IN (4003, 4056, 4017, 4143, 3858)
  )
)

INSERT overwrite TABLE ads.ads_vova_activity_daily_selection_v2 partition (pt='${pre_date}')
SELECT
  goods_id,
  region_id,
  'daily-selection' as biz_type,
  3 rp_type,
  first_cat_id,
  second_cat_id,
  rk
FROM
  tmp_no_brand_women_clothes

UNION ALL
SELECT
  goods_id,
  region_id,
  'daily-selection' as biz_type,
  3 rp_type,
  first_cat_id,
  second_cat_id,
  rk
FROM
  tmp_no_brand_no_women_clothes
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_activity_daily_selection" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi