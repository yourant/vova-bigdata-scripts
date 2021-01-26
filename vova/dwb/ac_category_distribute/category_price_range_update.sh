#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

sql="
WITH tmp_pay AS (
SELECT
  nvl ( fp.region_code, 'NALL' ) AS region_code,
  IF( platform IN ( 'pc', 'mob' ), 'web', platform ) AS platform,
  nvl ( dg.second_cat_name, 'NONAME' ) AS second_cat_name,
  fp.goods_number * fp.shop_price + fp.shipping_fee AS gmv,
  fp.goods_number AS sales_vol,
CASE
  WHEN fp.shop_price + fp.shipping_fee >= 0 AND fp.shop_price + fp.shipping_fee < 5 THEN 1
  WHEN fp.shop_price + fp.shipping_fee >= 5 AND fp.shop_price + fp.shipping_fee < 10 THEN 2
  WHEN fp.shop_price + fp.shipping_fee >= 10 AND fp.shop_price + fp.shipping_fee < 15
  THEN 3 WHEN fp.shop_price + fp.shipping_fee >= 15 AND fp.shop_price + fp.shipping_fee < 20
  THEN 4 WHEN fp.shop_price + fp.shipping_fee >= 20 AND fp.shop_price + fp.shipping_fee < 30
  THEN 5 WHEN fp.shop_price + fp.shipping_fee >= 30 AND fp.shop_price + fp.shipping_fee < 50
  THEN 6 WHEN fp.shop_price + fp.shipping_fee >= 50 AND fp.shop_price + fp.shipping_fee < 100
  THEN 7 WHEN fp.shop_price + fp.shipping_fee >= 100 THEN 8
END price_range
FROM
  dwd.dwd_vova_fact_pay fp
  LEFT JOIN dim.dim_vova_goods dg ON fp.goods_id = dg.goods_id
WHERE
  to_date ( fp.pay_time ) = '${cur_date}'
  AND fp.datasource = 'airyclub'
 -- AND fp.region_code IN ( 'GB', 'FR', 'DE', 'IT', 'ES' )
  ),
tmp_expre AS (
SELECT
  nvl ( gi.geo_country, 'NALL' ) AS region_code,
  IF(gi.os_type IN ( 'ios', 'android' ),gi.os_type,IF( gi.platform IN ( 'web', 'pc' ), 'web', 'unknow' ) ) AS platform,
  nvl ( dg.second_cat_name, 'NONAME' ) AS second_cat_name,
CASE
  WHEN dg.shop_price + dg.shipping_fee >= 0 AND dg.shop_price + dg.shipping_fee < 5 THEN 1
  WHEN dg.shop_price + dg.shipping_fee >= 5 AND dg.shop_price + dg.shipping_fee < 10 THEN 2
  WHEN dg.shop_price + dg.shipping_fee >= 10 AND dg.shop_price + dg.shipping_fee < 15 THEN 3
  WHEN dg.shop_price + dg.shipping_fee >= 15 AND dg.shop_price + dg.shipping_fee < 20 THEN 4
  WHEN dg.shop_price + dg.shipping_fee >= 20 AND dg.shop_price + dg.shipping_fee < 30 THEN 5
  WHEN dg.shop_price + dg.shipping_fee >= 30 AND dg.shop_price + dg.shipping_fee < 50 THEN 6
  WHEN dg.shop_price + dg.shipping_fee >= 50 AND dg.shop_price + dg.shipping_fee < 100 THEN 7
  WHEN dg.shop_price + dg.shipping_fee >= 100 THEN 8
END price_range
FROM
  dwd.dwd_vova_log_goods_impression gi
  LEFT JOIN dim.dim_vova_goods dg ON gi.virtual_goods_id = dg.virtual_goods_id
WHERE
  gi.pt = '${cur_date}'
  AND gi.dp = 'airyclub'
  -- AND gi.geo_country in ( 'GB', 'FR', 'DE', 'IT', 'ES' )
  ),
tmp_clk AS (
SELECT
  nvl ( gc.geo_country, 'NALL' ) AS region_code,
  IF(gc.os_type IN ( 'ios', 'android' ),gc.os_type,IF( gc.platform IN ( 'web', 'pc' ), 'web', 'unknow' ) ) AS platform,
  nvl ( dg.second_cat_name, 'NONAME' ) AS second_cat_name,
CASE
  WHEN dg.shop_price + dg.shipping_fee >= 0 AND dg.shop_price + dg.shipping_fee < 5 THEN 1
  WHEN dg.shop_price + dg.shipping_fee >= 5 AND dg.shop_price + dg.shipping_fee < 10 THEN 2
  WHEN dg.shop_price + dg.shipping_fee >= 10 AND dg.shop_price + dg.shipping_fee < 15 THEN 3
  WHEN dg.shop_price + dg.shipping_fee >= 15 AND dg.shop_price + dg.shipping_fee < 20 THEN 4
  WHEN dg.shop_price + dg.shipping_fee >= 20 AND dg.shop_price + dg.shipping_fee < 30 THEN 5
  WHEN dg.shop_price + dg.shipping_fee >= 30 AND dg.shop_price + dg.shipping_fee < 50 THEN 6
  WHEN dg.shop_price + dg.shipping_fee >= 50 AND dg.shop_price + dg.shipping_fee < 100 THEN 7
  WHEN dg.shop_price + dg.shipping_fee >= 100 THEN 8
END price_range
FROM
  dwd.dwd_vova_log_goods_click gc
  LEFT JOIN dim.dim_vova_goods dg ON gc.virtual_goods_id = dg.virtual_goods_id
WHERE
  gc.pt = '${cur_date}'
  AND gc.dp = 'airyclub'
  -- AND gc.geo_country in ( 'GB', 'FR', 'DE', 'IT', 'ES' )
  )
INSERT overwrite TABLE dwb.dwb_vova_ac_category_price_range_distribute PARTITION ( pt = '${cur_date}' )
SELECT
  'GMV',
  nvl ( region_code, 'all' ),
  nvl ( platform, 'all' ),
  nvl ( second_cat_name, 'all' ),
  cast( sum( IF ( price_range = 1, gmv, 0 ) ) AS DECIMAL ( 13, 2 ) ) AS gmv1,
  cast( sum( IF ( price_range = 2, gmv, 0 ) ) AS DECIMAL ( 13, 2 ) ) AS gmv2,
  cast( sum( IF ( price_range = 3, gmv, 0 ) ) AS DECIMAL ( 13, 2 ) ) AS gmv3,
  cast( sum( IF ( price_range = 4, gmv, 0 ) ) AS DECIMAL ( 13, 2 ) ) AS gmv4,
  cast( sum( IF ( price_range = 5, gmv, 0 ) ) AS DECIMAL ( 13, 2 ) ) AS gmv5,
  cast( sum( IF ( price_range = 6, gmv, 0 ) ) AS DECIMAL ( 13, 2 ) ) AS gmv6,
  cast( sum( IF ( price_range = 7, gmv, 0 ) ) AS DECIMAL ( 13, 2 ) ) AS gmv7,
  cast( sum( IF ( price_range = 8, gmv, 0 ) ) AS DECIMAL ( 13, 2 ) ) AS gmv8
FROM
  tmp_pay
GROUP BY
  region_code,
  platform,
  second_cat_name WITH cube

UNION ALL
SELECT
  'GMV占比',
  nvl ( region_code, 'all' ),
  nvl ( platform, 'all' ),
  nvl ( second_cat_name, 'all' ),
  concat(cast(sum( IF ( price_range = 1, gmv, 0 ) ) / sum( gmv ) * 100 AS DECIMAL ( 13, 2 ) ),'%' ) AS gmv_rate1,
  concat(cast(sum( IF ( price_range = 2, gmv, 0 ) ) / sum( gmv ) * 100 AS DECIMAL ( 13, 2 ) ),'%' ) AS gmv_rate2,
  concat(cast(sum( IF ( price_range = 3, gmv, 0 ) ) / sum( gmv ) * 100 AS DECIMAL ( 13, 2 ) ),'%' ) AS gmv_rate3,
  concat(cast(sum( IF ( price_range = 4, gmv, 0 ) ) / sum( gmv ) * 100 AS DECIMAL ( 13, 2 ) ),'%' ) AS gmv_rate4,
  concat(cast(sum( IF ( price_range = 5, gmv, 0 ) ) / sum( gmv ) * 100 AS DECIMAL ( 13, 2 ) ),'%' ) AS gmv_rate5,
  concat(cast(sum( IF ( price_range = 6, gmv, 0 ) ) / sum( gmv ) * 100 AS DECIMAL ( 13, 2 ) ),'%' ) AS gmv_rate6,
  concat(cast(sum( IF ( price_range = 7, gmv, 0 ) ) / sum( gmv ) * 100 AS DECIMAL ( 13, 2 ) ),'%' ) AS gmv_rate7,
  concat(cast(sum( IF ( price_range = 8, gmv, 0 ) ) / sum( gmv ) * 100 AS DECIMAL ( 13, 2 ) ),'%' ) AS gmv_rate8
FROM
  tmp_pay
GROUP BY
  region_code,
  platform,
  second_cat_name WITH cube

UNION ALL
SELECT
  '销量',
  nvl ( region_code, 'all' ),
  nvl ( platform, 'all' ),
  nvl ( second_cat_name, 'all' ),
  sum( IF ( price_range = 1, sales_vol, 0 ) ) AS sales_vol1,
  sum( IF ( price_range = 2, sales_vol, 0 ) ) AS sales_vol2,
  sum( IF ( price_range = 3, sales_vol, 0 ) ) AS sales_vol3,
  sum( IF ( price_range = 4, sales_vol, 0 ) ) AS sales_vol4,
  sum( IF ( price_range = 5, sales_vol, 0 ) ) AS sales_vol5,
  sum( IF ( price_range = 6, sales_vol, 0 ) ) AS sales_vol6,
  sum( IF ( price_range = 7, sales_vol, 0 ) ) AS sales_vol7,
  sum( IF ( price_range = 8, sales_vol, 0 ) ) AS sales_vol8
FROM
  tmp_pay
GROUP BY
  region_code,
  platform,
  second_cat_name WITH cube

UNION ALL
SELECT
  '销量占比',
  nvl ( region_code, 'all' ),
  nvl ( platform, 'all' ),
  nvl ( second_cat_name, 'all' ),
  concat(cast(sum( IF ( price_range = 1, sales_vol, 0 ) ) / sum( sales_vol ) * 100 AS DECIMAL ( 13, 2 )),'%') AS sales_vol_rate1,
  concat(cast(sum( IF ( price_range = 2, sales_vol, 0 ) ) / sum( sales_vol ) * 100 AS DECIMAL ( 13, 2 )),'%') AS sales_vol_rate2,
  concat(cast(sum( IF ( price_range = 3, sales_vol, 0 ) ) / sum( sales_vol ) * 100 AS DECIMAL ( 13, 2 )),'%') AS sales_vol_rate3,
  concat(cast(sum( IF ( price_range = 4, sales_vol, 0 ) ) / sum( sales_vol ) * 100 AS DECIMAL ( 13, 2 )),'%') AS sales_vol_rate4,
  concat(cast(sum( IF ( price_range = 5, sales_vol, 0 ) ) / sum( sales_vol ) * 100 AS DECIMAL ( 13, 2 )),'%') AS sales_vol_rate5,
  concat(cast(sum( IF ( price_range = 6, sales_vol, 0 ) ) / sum( sales_vol ) * 100 AS DECIMAL ( 13, 2 )),'%') AS sales_vol_rate6,
  concat(cast(sum( IF ( price_range = 7, sales_vol, 0 ) ) / sum( sales_vol ) * 100 AS DECIMAL ( 13, 2 )),'%') AS sales_vol_rate7,
  concat(cast(sum( IF ( price_range = 8, sales_vol, 0 ) ) / sum( sales_vol ) * 100 AS DECIMAL ( 13, 2 )),'%') AS sales_vol_rate8
FROM
  tmp_pay
GROUP BY
  region_code,
  platform,
  second_cat_name WITH cube
UNION ALL
SELECT
  '曝光量',
  nvl ( region_code, 'all' ),
  nvl ( platform, 'all' ),
  nvl ( second_cat_name, 'all' ),
  sum( IF ( price_range = 1, 1, 0 ) ) AS expre_cnt1,
  sum( IF ( price_range = 2, 1, 0 ) ) AS expre_cnt2,
  sum( IF ( price_range = 3, 1, 0 ) ) AS expre_cnt3,
  sum( IF ( price_range = 4, 1, 0 ) ) AS expre_cnt4,
  sum( IF ( price_range = 5, 1, 0 ) ) AS expre_cnt5,
  sum( IF ( price_range = 6, 1, 0 ) ) AS expre_cnt6,
  sum( IF ( price_range = 7, 1, 0 ) ) AS expre_cnt7,
  sum( IF ( price_range = 8, 1, 0 ) ) AS expre_cnt8
FROM
  tmp_expre
GROUP BY
  region_code,
  platform,
  second_cat_name WITH cube

UNION ALL
SELECT
  '曝光量占比',
  nvl ( region_code, 'all' ),
  nvl ( platform, 'all' ),
  nvl ( second_cat_name, 'all' ),
  concat(cast(sum( IF ( price_range = 1, 1, 0 ) ) / sum( 1 ) * 100 AS DECIMAL ( 13, 2 )),'%') AS expre_cnt_rate1,
  concat(cast(sum( IF ( price_range = 2, 1, 0 ) ) / sum( 1 ) * 100 AS DECIMAL ( 13, 2 )),'%') AS expre_cnt_rate2,
  concat(cast(sum( IF ( price_range = 3, 1, 0 ) ) / sum( 1 ) * 100 AS DECIMAL ( 13, 2 )),'%') AS expre_cnt_rate3,
  concat(cast(sum( IF ( price_range = 4, 1, 0 ) ) / sum( 1 ) * 100 AS DECIMAL ( 13, 2 )),'%') AS expre_cnt_rate4,
  concat(cast(sum( IF ( price_range = 5, 1, 0 ) ) / sum( 1 ) * 100 AS DECIMAL ( 13, 2 )),'%') AS expre_cnt_rate5,
  concat(cast(sum( IF ( price_range = 6, 1, 0 ) ) / sum( 1 ) * 100 AS DECIMAL ( 13, 2 )),'%') AS expre_cnt_rate6,
  concat(cast(sum( IF ( price_range = 7, 1, 0 ) ) / sum( 1 ) * 100 AS DECIMAL ( 13, 2 )),'%') AS expre_cnt_rate7,
  concat(cast(sum( IF ( price_range = 8, 1, 0 ) ) / sum( 1 ) * 100 AS DECIMAL ( 13, 2 )),'%') AS expre_cnt_rate8
FROM
  tmp_expre
GROUP BY
  region_code,
  platform,
  second_cat_name WITH cube

UNION ALL
SELECT
  '点击量',
  nvl ( region_code, 'all' ),
  nvl ( platform, 'all' ),
  nvl ( second_cat_name, 'all' ),
  sum( IF ( price_range = 1, 1, 0 ) ) AS clk_cnt1,
  sum( IF ( price_range = 2, 1, 0 ) ) AS clk_cnt2,
  sum( IF ( price_range = 3, 1, 0 ) ) AS clk_cnt3,
  sum( IF ( price_range = 4, 1, 0 ) ) AS clk_cnt4,
  sum( IF ( price_range = 5, 1, 0 ) ) AS clk_cnt5,
  sum( IF ( price_range = 6, 1, 0 ) ) AS clk_cnt6,
  sum( IF ( price_range = 7, 1, 0 ) ) AS clk_cnt7,
  sum( IF ( price_range = 8, 1, 0 ) ) AS clk_cnt8
FROM
  tmp_clk
GROUP BY
  region_code,
  platform,
  second_cat_name WITH cube

UNION ALL
SELECT
  '点击量占比',
  nvl ( region_code, 'all' ),
  nvl ( platform, 'all' ),
  nvl ( second_cat_name, 'all' ),
  concat(cast(sum( IF ( price_range = 1, 1, 0 ) ) / sum( 1 ) * 100 AS DECIMAL ( 13, 2 )),'%') AS clk_cnt_rate1,
  concat(cast(sum( IF ( price_range = 2, 1, 0 ) ) / sum( 1 ) * 100 AS DECIMAL ( 13, 2 )),'%') AS clk_cnt_rate2,
  concat(cast(sum( IF ( price_range = 3, 1, 0 ) ) / sum( 1 ) * 100 AS DECIMAL ( 13, 2 )),'%') AS clk_cnt_rate3,
  concat(cast(sum( IF ( price_range = 4, 1, 0 ) ) / sum( 1 ) * 100 AS DECIMAL ( 13, 2 )),'%') AS clk_cnt_rate4,
  concat(cast(sum( IF ( price_range = 5, 1, 0 ) ) / sum( 1 ) * 100 AS DECIMAL ( 13, 2 )),'%') AS clk_cnt_rate5,
  concat(cast(sum( IF ( price_range = 6, 1, 0 ) ) / sum( 1 ) * 100 AS DECIMAL ( 13, 2 )),'%') AS clk_cnt_rate6,
  concat(cast(sum( IF ( price_range = 7, 1, 0 ) ) / sum( 1 ) * 100 AS DECIMAL ( 13, 2 )),'%') AS clk_cnt_rate7,
  concat(cast(sum( IF ( price_range = 8, 1, 0 ) ) / sum( 1 ) * 100 AS DECIMAL ( 13, 2 )),'%') AS clk_cnt_rate8
FROM
  tmp_clk
GROUP BY
  region_code,
  platform,
second_cat_name WITH cube
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=dwb_vova_ac_category_price_range_distribute" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 280" \
--conf "spark.sql.shuffle.partitions=280" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi