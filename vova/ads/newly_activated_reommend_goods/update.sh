#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
INSERT overwrite TABLE ads.ads_vova_newly_activated_recommend_goods PARTITION (pt = '${pre_date}')
SELECT /*+ REPARTITION(1) */
       dg.goods_id,
       nvl(dg.first_cat_id, 0) AS first_cat_id,
       nvl(dg.second_cat_id, 0) AS second_cat_id
FROM (
         SELECT DISTINCT dg.goods_id
         FROM dim.dim_vova_goods dg
                  INNER JOIN
              (
                  SELECT mr.mct_id,
                         mr.first_cat_id
                  FROM ads.ads_vova_mct_rank mr
                  WHERE mr.rank = 5
                    AND mr.pt = '${pre_date}'
                  UNION
                  SELECT mr.mct_id,
                         mr.first_cat_id
                  FROM ads.ads_vova_six_rank_mct mr
              ) mr ON mr.mct_id = dg.mct_id AND mr.first_cat_id = dg.first_cat_id

         UNION

         SELECT DISTINCT goods_id
         FROM dim.dim_vova_virtual_six_mct_goods

         UNION

         SELECT DISTINCT dg.goods_id
         FROM dim.dim_vova_goods dg
                  INNER JOIN ads.ads_vova_goods_portrait gp ON gp.gs_id = dg.goods_id
                  INNER JOIN ods_vova_vts.ods_vova_goods_comment vgc ON vgc.goods_id = dg.goods_id
         WHERE gp.pt = '${pre_date}'
           AND dg.is_on_sale = 1
           AND (gp.nlrf_rate_5_8w IS NULL OR gp.nlrf_rate_5_8w < 0.05)
     ) fin
         INNER JOIN dim.dim_vova_goods dg ON fin.goods_id = dg.goods_id
WHERE dg.is_on_sale = 1
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_newly_activated_recommend_goods" \
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
