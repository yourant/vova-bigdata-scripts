#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
WITH tmp_activity_no_brand_goods_pool AS (
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
    join mlb.mlb_vova_rec_b_goods_score_d p on cb.goods_id=p.goods_id
WHERE
    cb.pt = '${pre_date}'
    AND p.pt = '${pre_date}'
    AND cb.clk_cnt >0
    AND cb.is_brand = 0
    AND p.overall_score>30
    AND cb.region_id IN ( 0, 3858, 4003, 4017, 4056, 4143 )
)

INSERT overwrite TABLE ads.ads_vova_activity_no_brand_goods_pool PARTITION ( pt = '${pre_date}' )
SELECT
goods_id,
region_id,
biz_type,
rp_type,
first_cat_id,
second_cat_id,
rank
from (
  SELECT
  goods_id,
  region_id,
  'not_brand' as biz_type,
  3 AS rp_type,
  first_cat_id,
  nvl ( second_cat_id, 0 ) AS second_cat_id,
  row_number ( ) over ( PARTITION BY region_id ORDER BY ord_cnt/click_uv DESC ) AS rank
  FROM
      tmp_activity_no_brand_goods_pool
) where rank<=10000;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_activity_no_brand_goods_pool" \
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