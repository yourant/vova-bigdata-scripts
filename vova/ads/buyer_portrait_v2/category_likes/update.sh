#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
ALTER TABLE ads.ads_buyer_portrait_category_likes_weight DROP if exists partition(pt = '$(date -d "${pre_date:0:10} -180day" +%Y-%m-%d)');
INSERT overwrite TABLE ads.ads_buyer_portrait_category_likes_weight PARTITION ( pt = '${pre_date}' )
SELECT
  nvl(t1.buyer_id,lw.buyer_id),
  nvl(t1.cat_id,lw.cat_id),
  nvl(t1.expre_cnt,0) *- 0.001+ nvl(t1.clk_valid_cnt,0) * 1 + nvl(t1.collect_cnt,0) * 4+ nvl(t1.add_cat_cnt,0) * 5+ nvl(t1.ord_cnt,0) * 7 + nvl(lw.likes_weight,0)*0.95 as likes_weight
FROM
  (
SELECT
  buyer_id,
  cat_id,
  sum( expre_cnt ) AS expre_cnt,
  sum( clk_cnt ) AS clk_cnt,
  sum( clk_valid_cnt ) AS clk_valid_cnt,
  sum( collect_cnt ) AS collect_cnt,
  sum( add_cat_cnt ) AS add_cat_cnt,
  sum( ord_cnt ) AS ord_cnt
FROM
  dws.dws_buyer_goods_behave gb
WHERE
  pt = '${pre_date}'
GROUP BY
  buyer_id,
  cat_id
  ) t1
  FULL JOIN (SELECT* from ads.ads_buyer_portrait_category_likes_weight where pt = to_date ( date_sub( '${pre_date}', 1 ))) lw
  ON t1.buyer_id = lw.buyer_id
  AND t1.cat_id = lw.cat_id
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_buyer_portrait_category_likes_1" \
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

sql="
ALTER TABLE ads.ads_buyer_portrait_category_likes DROP if exists partition(pt = '$(date -d "${pre_date:0:10} -180day" +%Y-%m-%d)');
insert overwrite table ads.ads_buyer_portrait_category_likes partition(pt='$pre_date')
SELECT
  /*+ REPARTITION(30) */
  buyer_id,
  cat_id,
  sum( IF ( day_gap < 7, expre_cnt, 0 ) ) AS expre_cnt_1w,
  sum( IF ( day_gap < 15, expre_cnt, 0 ) ) AS expre_cnt_15d,
  sum( IF ( day_gap < 30, expre_cnt, 0 ) ) AS expre_cnt_1m,
  sum( IF ( day_gap < 7, clk_cnt, 0 ) ) AS clk_cnt_1w,
  sum( IF ( day_gap < 15, clk_cnt, 0 ) ) AS clk_cnt_15d,
  sum( IF ( day_gap < 30, clk_cnt, 0 ) ) AS clk_cnt_1m,
  sum( IF ( day_gap < 7, clk_valid_cnt, 0 ) ) AS clk_valid_cnt_1w,
  sum( IF ( day_gap < 15, clk_valid_cnt, 0 ) ) AS clk_valid_cnt_15d,
  sum( IF ( day_gap < 30, clk_valid_cnt, 0 ) ) AS clk_valid_cnt_1m,
  sum( IF ( day_gap < 7, collect_cnt, 0 ) ) AS collect_cnt_1w,
  sum( IF ( day_gap < 15, collect_cnt, 0 ) ) AS collect_cnt_15d,
  sum( IF ( day_gap < 30, collect_cnt, 0 ) ) AS collect_cnt_1m,
  sum( IF ( day_gap < 7, add_cat_cnt, 0 ) ) AS add_cat_cnt_1w,
  sum( IF ( day_gap < 15, add_cat_cnt, 0 ) ) AS add_cat_cnt_15d,
  sum( IF ( day_gap < 30, add_cat_cnt, 0 ) ) AS add_cat_cnt_1m,
  sum( IF ( day_gap < 7, ord_cnt, 0 ) ) AS ord_cnt_1w,
  sum( IF ( day_gap < 15, ord_cnt, 0 ) ) AS ord_cnt_15d,
  sum( IF ( day_gap < 30, ord_cnt, 0 ) ) AS ord_cnt_1m
FROM
  (
SELECT
  buyer_id,
  cat_id,
  pt,
  datediff( '${pre_date}', pt ) AS day_gap,
  sum( expre_cnt ) AS expre_cnt,
  sum( clk_cnt ) AS clk_cnt,
  sum( clk_valid_cnt ) AS clk_valid_cnt,
  sum( collect_cnt ) AS collect_cnt,
  sum( add_cat_cnt ) AS add_cat_cnt,
  sum( ord_cnt ) AS ord_cnt
FROM
  dws.dws_buyer_goods_behave
WHERE
  pt > date_sub( '${pre_date}', 30 )
  AND pt <= '${pre_date}'
GROUP BY
  buyer_id,
  cat_id,
  pt
  )
GROUP BY
  buyer_id,
  cat_id;
INSERT OVERWRITE TABLE ads.ads_buyer_portrait_category_likes_with_click_15d
SELECT
/*+ REPARTITION(10) */
buyer_id,
cat_id,
expre_cnt_1w,
expre_cnt_15d,
expre_cnt_1m,
clk_cnt_1w,
clk_cnt_15d,
clk_cnt_1m,
clk_valid_cnt_1w,
clk_valid_cnt_15d,
clk_valid_cnt_1m,
collect_cnt_1w,
collect_cnt_15d,
collect_cnt_1m,
add_cat_cnt_1w,
add_cat_cnt_15d,
add_cat_cnt_1m,
ord_cnt_1w,
ord_cnt_15d,
ord_cnt_1m
FROM
ads.ads_buyer_portrait_category_likes
where pt='${pre_date}' and clk_cnt_15d >0;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_buyer_portrait_category_likes_2" \
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
