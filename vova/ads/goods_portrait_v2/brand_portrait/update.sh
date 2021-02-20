#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
INSERT overwrite TABLE ads.ads_vova_brand_portrait partition(pt='$pre_date')
SELECT
/*+ REPARTITION(30) */
tmp_beh.second_cat_id,
tmp_beh.brand_id,
tmp_beh.clk_cnt_1w,
tmp_beh.clk_cnt_15d,
tmp_beh.clk_cnt_1m,
tmp_beh.collect_cnt_1w,
tmp_beh.collect_cnt_15d,
tmp_beh.collect_cnt_1m,
tmp_beh.add_cat_cnt_1w,
tmp_beh.add_cat_cnt_15d,
tmp_beh.add_cat_cnt_1m,
tmp_beh.sales_vol_1w,
tmp_beh.sales_vol_15d,
tmp_beh.sales_vol_1m,
tmp_beh.gmv_1w,
tmp_beh.gmv_15d,
tmp_beh.gmv_1m,
nvl (tmp_beh.clk_cnt_1w/tmp_beh.expre_cnt_1w*100,0),
nvl (tmp_beh.clk_cnt_15d/tmp_beh.expre_cnt_15d*100,0),
nvl (tmp_beh.clk_cnt_1m/tmp_beh.expre_cnt_1m*100,0),
nvl (tmp_clk.clk_uv_1w/tmp_expre.expre_uv_1w*100,0),
nvl (tmp_clk.clk_uv_15d/tmp_expre.expre_uv_15d*100,0),
nvl (tmp_clk.clk_uv_1m/tmp_expre.expre_uv_1m*100,0)
from

   (SELECT
      second_cat_id,
      brand_id,
      sum( IF ( day_gap < 7, expre_cnt, 0 ) ) AS expre_cnt_1w,
      sum( IF ( day_gap < 15, expre_cnt, 0 ) ) AS expre_cnt_15d,
      sum( IF ( day_gap < 30, expre_cnt, 0 ) ) AS expre_cnt_1m,
      sum( IF ( day_gap < 7, clk_cnt, 0 ) ) AS clk_cnt_1w,
      sum( IF ( day_gap < 15, clk_cnt, 0 ) ) AS clk_cnt_15d,
      sum( IF ( day_gap < 30, clk_cnt, 0 ) ) AS clk_cnt_1m,
      sum( IF ( day_gap < 7, collect_cnt, 0 ) ) AS collect_cnt_1w,
      sum( IF ( day_gap < 15, collect_cnt, 0 ) ) AS collect_cnt_15d,
      sum( IF ( day_gap < 30, collect_cnt, 0 ) ) AS collect_cnt_1m,
      sum( IF ( day_gap < 7, add_cat_cnt, 0 ) ) AS add_cat_cnt_1w,
      sum( IF ( day_gap < 15, add_cat_cnt, 0 ) ) AS add_cat_cnt_15d,
      sum( IF ( day_gap < 30, add_cat_cnt, 0 ) ) AS add_cat_cnt_1m,
      sum( IF ( day_gap < 7, sales_vol, 0 ) ) AS sales_vol_1w,
      sum( IF ( day_gap < 15, sales_vol, 0 ) ) AS sales_vol_15d,
      sum( IF ( day_gap < 30, sales_vol, 0 ) ) AS sales_vol_1m,
      sum( IF ( day_gap < 7, gmv, 0 ) ) AS gmv_1w,
      sum( IF ( day_gap < 15, gmv, 0 ) ) AS gmv_15d,
      sum( IF ( day_gap < 30, gmv, 0 ) ) AS gmv_1m
    FROM
      (
    SELECT
      second_cat_id,
      brand_id,
      pt,
      datediff( '${pre_date}', pt ) AS day_gap,
      sum( expre_cnt ) AS expre_cnt,
      sum( clk_cnt ) AS clk_cnt,
      sum( clk_valid_cnt ) AS clk_valid_cnt,
      sum( collect_cnt ) AS collect_cnt,
      sum( add_cat_cnt ) AS add_cat_cnt,
      sum( sales_vol ) AS sales_vol,
      sum( gmv ) AS gmv
    FROM
      dws.dws_vova_buyer_goods_behave
    WHERE
      pt > date_sub( '${pre_date}', 30 )
      AND pt <= '${pre_date}'
    GROUP BY
      second_cat_id,
      brand_id,
      pt
     )
   GROUP BY
     second_cat_id,brand_id
     )tmp_beh

  LEFT JOIN -- 曝光uv
  (
    SELECT
      t1.second_cat_id,
      t1.brand_id,
      count( DISTINCT IF ( t1.day_gap < 7, device_id, NULL ) ) AS expre_uv_1w,
      count( DISTINCT IF ( t1.day_gap < 15, device_id, NULL ) ) AS expre_uv_15d,
      count( DISTINCT IF ( t1.day_gap < 30, device_id, NULL ) ) AS expre_uv_1m
    FROM
      (
    SELECT
      dg.second_cat_id,
      dg.brand_id,
      gi.device_id,
      datediff( '${pre_date}', pt ) AS day_gap
    FROM
      dwd.dwd_vova_log_goods_impression gi
      INNER JOIN dim.dim_vova_goods dg ON dg.virtual_goods_id = gi.virtual_goods_id
    WHERE
      gi.pt > date_sub( '${pre_date}', 30 )
      AND gi.pt <= '${pre_date}'
      AND platform = 'mob'
      ) t1
    GROUP BY
      t1.second_cat_id,t1.brand_id
  ) tmp_expre
  ON tmp_beh.second_cat_id = tmp_expre.second_cat_id and tmp_beh.brand_id=tmp_expre.brand_id
  LEFT JOIN -- 点击uv
  (
    SELECT
      t1.second_cat_id,
      t1.brand_id,
      count( DISTINCT IF ( t1.day_gap < 7, device_id, NULL ) ) AS clk_uv_1w,
      count( DISTINCT IF ( t1.day_gap < 15, device_id, NULL ) ) AS clk_uv_15d,
      count( DISTINCT IF ( t1.day_gap < 30, device_id, NULL ) ) AS clk_uv_1m
    FROM
      (
    SELECT
      dg.second_cat_id,
      dg.brand_id,
      gc.device_id,
      datediff( '${pre_date}', pt ) AS day_gap
    FROM
      dwd.dwd_vova_log_goods_click gc
      INNER JOIN dim.dim_vova_goods dg ON dg.virtual_goods_id = gc.virtual_goods_id
    WHERE
      gc.pt > date_sub( '${pre_date}', 30 )
      AND gc.pt <= '${pre_date}'
      AND platform = 'mob'
      ) t1
    GROUP BY
      t1.second_cat_id,t1.brand_id
  ) tmp_clk
  ON tmp_beh.second_cat_id = tmp_clk.second_cat_id and tmp_beh.brand_id=tmp_clk.brand_id
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_brand_portrait" \
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
