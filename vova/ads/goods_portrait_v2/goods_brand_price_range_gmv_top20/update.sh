#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
INSERT OVERWRITE TABLE ads.ads_vova_goods_portrait_brand_price_range_likes_top20
select
goods_id,
brand_id,
price_range,
rk
from
(select
tmp1.goods_id,
dg.brand_id,
tmp1.price_range,
row_number ( ) over ( PARTITION BY dg.brand_id, tmp1.price_range ORDER BY tmp1.cr  DESC ) rk
from
(SELECT
    gb.gs_id AS goods_id,
    gb.price_range,
    sum( gb.expre_cnt ) AS expre_cnt,
    sum( gb.ord_cnt ) AS ord_cnt,
    sum( gmv ) AS gmv,
    sum( gb.clk_cnt )/sum( gb.expre_cnt ) AS cr,
    sum( gb.expre_cnt )/count( DISTINCT IF ( gb.clk_cnt > 0, gb.buyer_id, NULL ) )*sum( gb.clk_cnt )/sum( gb.expre_cnt )*10000 AS gcr
FROM
    dws.dws_vova_buyer_goods_behave gb
WHERE
    gb.pt <= '${pre_date}' AND gb.pt > date_sub( '${pre_date}', 7 )
GROUP BY
    gb.gs_id,
    gb.price_range
HAVING
    gmv>50
    AND ord_cnt>5
    and expre_cnt >= 10000
    AND cr  >= 0.02
    AND gcr > 50)tmp1
inner join dim.dim_vova_goods dg
on tmp1.goods_id = dg.goods_id)
where rk <= 50
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--driver-memory 5G \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=20" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_goods_portrait_brand_price_range_likes_top20" \
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
