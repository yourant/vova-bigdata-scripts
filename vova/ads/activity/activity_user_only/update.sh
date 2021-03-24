#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
-- req8811
WITH ads_activity_user_only_tmp_gooods AS (
SELECT
    cb.goods_id,
    dg.first_cat_id,
    dg.second_cat_id,
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
    INNER JOIN dim.dim_vova_goods dg ON cb.goods_id = dg.goods_id
WHERE
    cb.pt = '${pre_date}'
    AND cb.clk_cnt >0
    AND (dg.shop_price+dg.shipping_fee) <= 3.6
    AND cb.region_id IN ( 0, 3858, 4003, 4017, 4056, 4143 ))

INSERT overwrite TABLE ads.ads_vova_activity_user_only PARTITION ( pt = '${pre_date}' )
select
*
from
(SELECT
    goods_id,
    region_id,
    'new-user-only' AS biz_type,
    3 AS rp_type,
    first_cat_id,
    nvl ( second_cat_id, 0 ) AS second_cat_id,
    row_number ( ) over ( PARTITION BY region_id ORDER BY ord_cnt / expre_cnt DESC ) rank
FROM
ads_activity_user_only_tmp_gooods)
where rank<=10000
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_activity_user_only" \
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