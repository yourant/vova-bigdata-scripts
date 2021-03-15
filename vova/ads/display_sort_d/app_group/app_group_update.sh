#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
sql="
INSERT OVERWRITE TABLE ads.ads_vova_app_group_display_sort PARTITION (pt = '${cur_date}')
SELECT /*+ REPARTITION(1) */
    fin.goods_id,
    CASE
        WHEN region_code = 'GB' THEN 3858
        WHEN region_code = 'FR' THEN 4003
        WHEN region_code = 'IT' THEN 4056
        WHEN region_code = 'DE' THEN 4017
        WHEN region_code = 'excellent' THEN 0
        WHEN region_code = 'normal' THEN 0
        ELSE 0 END       AS region_id,
    CASE
        WHEN region_code IN ('GB', 'FR', 'IT', 'DE', 'excellent') THEN 'excellent'
        WHEN region_code = 'normal' THEN 'normal'
        ELSE 'error' END AS region_standard_type,
    fin.gcr_rank_desc
FROM (
         SELECT goods_id,
                region_code,
                gcr_rank_desc
         FROM (
                  SELECT vgp.goods_id,
                         vgp.region_code,
                         row_number() OVER(PARTITION BY vgp.region_code ORDER BY vgp.gcr DESC) AS gcr_rank_desc
                  FROM ads.ads_vova_goods_performance vgp
                  WHERE vgp.pt = '${cur_date}'
                    AND vgp.datasource = 'airyclub'
                    AND vgp.platform = 'all'
                    AND vgp.region_code IN ('FR', 'DE', 'IT')
                    AND vgp.impressions > 5000
                    AND vgp.ctr > 0.02
                    AND vgp.rate > 0.03
                    AND vgp.gcr > 60
              ) t1
         WHERE gcr_rank_desc <= 500

         UNION ALL

         SELECT goods_id,
                region_code,
                gcr_rank_desc
         FROM (
                  SELECT vgp.goods_id,
                         vgp.region_code,
                         row_number() OVER(PARTITION BY vgp.region_code ORDER BY vgp.gcr DESC) AS gcr_rank_desc
                  FROM ads.ads_vova_goods_performance vgp
                  WHERE vgp.pt = '${cur_date}'
                    AND vgp.datasource = 'airyclub'
                    AND vgp.platform = 'all'
                    AND vgp.region_code IN ('GB')
                    AND vgp.impressions > 5000
                    AND vgp.ctr > 0.02
                    AND vgp.rate > 0.02
                    AND vgp.gcr > 60
              ) t1
         WHERE gcr_rank_desc <= 500

         UNION ALL

         SELECT goods_id,
                'excellent' AS region_code,
                gcr_rank_desc
         FROM (
                  SELECT vgp.goods_id,
                         vgp.region_code,
                         row_number() OVER(PARTITION BY vgp.region_code ORDER BY vgp.gcr DESC) AS gcr_rank_desc
                  FROM ads.ads_vova_goods_performance vgp
                  WHERE vgp.pt = '${cur_date}'
                    AND vgp.datasource = 'airyclub'
                    AND vgp.platform = 'all'
                    AND vgp.region_code IN ('all')
                    AND vgp.impressions > 5000
                    AND vgp.ctr > 0.02
                    AND vgp.rate > 0.03
                    AND vgp.gcr > 60
              ) t1
         WHERE gcr_rank_desc <= 500

         UNION ALL

         SELECT t1.goods_id,
                'normal' AS region_code,
                t1.gcr_rank_desc
         FROM (
                  SELECT vgp.goods_id,
                         row_number() OVER(PARTITION BY vgp.region_code ORDER BY vgp.gcr DESC) AS gcr_rank_desc
                  FROM ads.ads_vova_goods_performance vgp
                           LEFT JOIN
                       (
                           SELECT goods_id
                           FROM (
                                    SELECT vgp.goods_id,
                                           vgp.region_code,
                                           row_number() OVER(PARTITION BY vgp.region_code ORDER BY vgp.gcr DESC) AS gcr_rank_desc
                                    FROM ads.ads_vova_goods_performance vgp
                                    WHERE vgp.pt = '${cur_date}'
                                      AND vgp.datasource = 'airyclub'
                                      AND vgp.platform = 'all'
                                      AND vgp.region_code IN ('all')
                                      AND vgp.impressions > 5000
                                      AND vgp.ctr > 0.02
                                      AND vgp.rate > 0.03
                                      AND vgp.gcr > 60
                                ) t1
                           WHERE gcr_rank_desc <= 500
                       ) excellent_data ON excellent_data.goods_id = vgp.goods_id
                  WHERE vgp.pt = '${cur_date}'
                    AND vgp.datasource = 'airyclub'
                    AND vgp.platform = 'all'
                    AND vgp.region_code IN ('all')
                    AND vgp.impressions > 500
                    AND vgp.ctr > 0.005
                    AND vgp.rate > 0.005
                    AND vgp.gcr > 30
                    AND excellent_data.goods_id is null
              ) t1
         WHERE t1.gcr_rank_desc <= 500
     ) fin

;
"


#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=10" \
--conf "spark.dynamicAllocation.maxExecutors=50" \
--conf "spark.app.name=ads_vova_app_group_display_sort" \
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
