#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天

if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

pre_month=`date -d $pre_date"-1 month" +%Y-%m-%d`

sql="
INSERT OVERWRITE TABLE ads.ads_vova_default_brand_cat_likes_weight
SELECT
    1 AS type,
    first_cat_id AS biz_id,
    cast( sum( gmv ) AS BIGINT ) AS count
FROM
    dws.dws_vova_buyer_goods_behave
WHERE
    pt > '${pre_month}'
    AND pt <= '${pre_date}'
    AND first_cat_id is not null
GROUP BY
    first_cat_id

UNION ALL

SELECT
    2 AS type,
    second_cat_id AS biz_id,
    cast( sum( gmv ) AS BIGINT ) AS count
FROM
    dws.dws_vova_buyer_goods_behave
WHERE
    pt > '${pre_month}'
    AND pt <= '${pre_date}'
    AND second_cat_id is not null
GROUP BY
    second_cat_id

UNION ALL

SELECT
    3 AS type,
    brand_id AS biz_id,
    cast( sum( gmv ) AS BIGINT ) AS count
FROM
    dws.dws_vova_buyer_goods_behave
WHERE
    pt > '${pre_month}'
    AND pt <= '${pre_date}'
    AND brand_id is not null
GROUP BY
    brand_id
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=200" \
--conf "spark.app.name=ads_vova_default_brand_cat_likes_weight" \
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