#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
ALTER TABLE ads.ads_buyer_portrait_brand_price_range_likes_weight DROP if exists partition(pt = '$(date -d "${pre_date:0:10} -180day" +%Y-%m-%d)');
INSERT overwrite TABLE ads.ads_buyer_portrait_brand_price_range_likes_weight PARTITION ( pt = '${pre_date}' )
SELECT
  /*+ repartition(30) */
  nvl(t1.buyer_id,lw.buyer_id),
  nvl(t1.brand_id,lw.brand_id),
  nvl(t1.price_range,lw.price_range),
  nvl(t1.expre_cnt,0) *- 0.001+ nvl(t1.clk_valid_cnt,0) * 1 + nvl(t1.collect_cnt,0) * 4+ nvl(t1.add_cat_cnt,0) * 5+ nvl(t1.ord_cnt,0) * 7 + nvl(lw.likes_weight,0)*0.95 as likes_weight
FROM
  (
SELECT
  buyer_id,
  brand_id,
  price_range,
  sum( expre_cnt ) AS expre_cnt,
  sum( clk_cnt ) AS clk_cnt,
  sum( clk_valid_cnt ) AS clk_valid_cnt,
  sum( collect_cnt ) AS collect_cnt,
  sum( add_cat_cnt ) AS add_cat_cnt,
  sum( ord_cnt ) AS ord_cnt
FROM
  dws.dws_buyer_goods_behave gb
WHERE
  pt = '${pre_date}'  and buyer_id>0 and brand_id>0 and buyer_id>0
GROUP BY
  buyer_id,
  brand_id,
  price_range
  ) t1
  FULL JOIN (SELECT * from ads.ads_buyer_portrait_brand_price_range_likes_weight where pt = to_date ( date_sub( '${pre_date}', 1 ))) lw
  ON t1.buyer_id = lw.buyer_id
  AND t1.brand_id = lw.brand_id
  AND t1.price_range = lw.price_range;

insert overwrite table ads.ads_buyer_portrait_brand_price_range_likes_top10
select
buyer_id,
brand_id,
price_range,
rk
from
(select
buyer_id,
brand_id,
price_range,
row_number() over(partition by buyer_id order by likes_weight desc) rk
from
ads.ads_buyer_portrait_brand_price_range_likes_weight  where pt='${pre_date}' and likes_weight>=1
)
where rk<=10;


"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_buyer_portrait_brand_price_range_likes_weight" \
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