#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
-- req5703
WITH ads_vova_activity_outlets_tmp_gooods AS (
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
    cb.sales_vol,
    cb.is_brand
FROM
    dwd.dwd_vova_activity_goods_ctry_behave cb
WHERE
    cb.pt='${pre_date}'
    AND cb.expre_cnt >= 500
    AND cb.expre_cnt <= 200000
    AND cb.clk_cnt/cb.expre_cnt>0.02
    AND cb.ord_cnt/cb.click_uv >0.03
    AND cb.region_id IN ( 0, 3858, 4003, 4017, 4056, 4143 )),

tmp_brand (
SELECT
    2 AS event_type,
    region_id,
    first_cat_id,
    second_cat_id,
    goods_id,
    ord_cnt / expre_cnt AS cr
FROM
    ads_vova_activity_outlets_tmp_gooods
WHERE
    is_brand = 1
    AND ( ( region_id = 4003 AND gmv >= 50 ) OR ( region_id != 4003 AND gmv >= 20 ) )
    AND (
    ( first_cat_id = 5777 AND gmv / click_uv * clk_cnt / expre_cnt * 10000 >= 200 )
    OR ( first_cat_id != 5777 AND gmv / click_uv * clk_cnt / expre_cnt * 10000 >= 120 )
    )
    ),
    tmp_no_brand (
SELECT
    4 AS event_type,
    region_id,
    first_cat_id,
    second_cat_id,
    goods_id,
    ord_cnt / expre_cnt AS cr
FROM
    ads_vova_activity_outlets_tmp_gooods
WHERE
    is_brand = 0
    AND ( ( region_id = 4003 AND gmv >= 20 ) OR ( region_id != 4003 AND gmv >= 10 ) )
    AND (
    ( first_cat_id = 5777 AND gmv / click_uv * clk_cnt / expre_cnt * 10000 >= 150 )
    OR ( first_cat_id != 5777 AND gmv / click_uv * clk_cnt / expre_cnt * 10000 >= 60 )
    )
    ),
    tmp_second_cat (
SELECT
    5 AS event_type,
    region_id,
    first_cat_id,
    second_cat_id,
    goods_id,
    ord_cnt / expre_cnt AS cr
FROM
    ads_vova_activity_outlets_tmp_gooods
WHERE
    is_brand = 1
    AND gmv >= 20
    AND (
    ( first_cat_id = 5777 AND gmv / click_uv * clk_cnt / expre_cnt * 10000 >= 200 )
    OR ( first_cat_id != 5777 AND gmv / click_uv * clk_cnt / expre_cnt * 10000 >= 120 )
    )
    )

INSERT overwrite TABLE ads.ads_vova_activity_outlets  partition (pt='${pre_date}')
SELECT
    'outlets' as biz_type,
    event_type,
    region_id,
    first_cat_id,
    second_cat_id,
    goods_id,
    rank
FROM
    (
SELECT
    tmp3.event_type,
    tmp3.region_id,
    tmp3.first_cat_id,
    tmp3.second_cat_id,
    tmp3.goods_id,
    row_number ( ) over ( PARTITION BY tmp3.event_type, tmp3.region_id ORDER BY tmp3.cr DESC ) rank
FROM
    (
SELECT
    tmp1.event_type,
    tmp1.region_id,
    tmp1.first_cat_id,
    tmp1.second_cat_id,
    nvl ( tmp2.min_price_goods_id, tmp1.goods_id ) AS goods_id,
    tmp1.cr,
    row_number ( ) over (
    PARTITION BY tmp1.event_type,
    tmp1.region_id,
    nvl ( tmp2.min_price_goods_id, tmp1.goods_id )
ORDER BY
    tmp1.cr DESC
    ) grank
FROM
    (
SELECT
    event_type,
    region_id,
    first_cat_id,
    second_cat_id,
    goods_id,
    cr
FROM
    tmp_brand

UNION ALL
SELECT
    event_type,
    region_id,
    first_cat_id,
    second_cat_id,
    goods_id,
    cr
FROM
    tmp_no_brand

UNION ALL
SELECT
    event_type,
    region_id,
    first_cat_id,
    second_cat_id,
    goods_id,
    cr
FROM
    tmp_second_cat
    ) tmp1
    LEFT JOIN (
SELECT
    mpg.goods_id,
    mpg.min_price_goods_id,
    dg.second_cat_id
FROM
    ads.ads_vova_min_price_goods_d mpg
    LEFT JOIN dim.dim_vova_goods dg ON mpg.min_price_goods_id = dg.goods_id
WHERE
    pt = '${pre_date}'
    AND strategy = 'c'
    ) tmp2 ON tmp1.goods_id = tmp2.goods_id
    AND tmp1.second_cat_id = tmp2.second_cat_id
    ) tmp3
WHERE
    grank = 1
    )
WHERE
    rank <= 1000
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_activity_outlets" \
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