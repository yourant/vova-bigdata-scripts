#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
-- req7429
WITH tmp_all_type (
SELECT
    dc.cat_id,
    tmp_country.region_id,
    explode ( split ( '1,2,3', ',' ) ) AS gender,
    1 AS type
FROM
    dim.dim_vova_category dc
    LEFT JOIN (
SELECT
    region_id,
    sum( shop_price * goods_number + shipping_fee ) AS gmv
FROM
    dwd.dwd_vova_fact_pay fp
WHERE
    date( fp.pay_time ) <= '${pre_date}' AND date( fp.pay_time ) > date_sub( '${pre_date}', 7 )
GROUP BY
    region_id
HAVING
    gmv >= 10000

UNION ALL
SELECT
    0 AS region_id,
    0 AS gmv
    ) tmp_country
WHERE
    depth = 1 UNION ALL
SELECT
    dc.cat_id,
    tmp_country.region_id,
    explode ( split ( '1,2,3', ',' ) ) AS gender,
    2 AS type
FROM
    dim.dim_vova_category dc
    LEFT JOIN (
SELECT
    region_id,
    sum( shop_price * goods_number + shipping_fee ) AS gmv
FROM
    dwd.dwd_vova_fact_pay fp
WHERE
    date( fp.pay_time ) <= '${pre_date}' AND date( fp.pay_time ) > date_sub( '${pre_date}', 7 )
GROUP BY
    region_id
HAVING
    gmv >= 10000

UNION ALL
SELECT
    0 AS region_id,
    0 AS gmv
    ) tmp_country
WHERE
    depth = 2
    ),
    tmp_gmv (
SELECT
    cat_id,
    type,
    nvl ( region_id, 0 ) AS region_id,
    gender,
    sum( gmv ) AS gmv
FROM
    (
SELECT
    dg.first_cat_id AS cat_id,
    1 AS type,
    fp.region_id,
CASE

    WHEN db.gender = 'male' THEN
    1
    WHEN db.gender = 'female' THEN
    2 ELSE 3
    END AS gender,
    fp.shop_price * fp.goods_number + fp.shipping_fee AS gmv
FROM
    dwd.dwd_vova_fact_pay fp
    INNER JOIN dim.dim_vova_goods dg ON fp.goods_id = dg.goods_id
    INNER JOIN dim.dim_vova_buyers db ON fp.buyer_id = db.buyer_id
WHERE
    date( fp.pay_time ) <= '${pre_date}' AND date( fp.pay_time ) > date_sub( '${pre_date}', 7 ) UNION ALL
SELECT
    dg.second_cat_id AS cat_id,
    2 AS type,
    fp.region_id,
CASE

        WHEN db.gender = 'male' THEN
        1
        WHEN db.gender = 'female' THEN
        2 ELSE 3
    END AS gender,
    fp.shop_price * fp.goods_number + fp.shipping_fee AS gmv
FROM
    dwd.dwd_vova_fact_pay fp
    INNER JOIN dim.dim_vova_goods dg ON fp.goods_id = dg.goods_id
    INNER JOIN dim.dim_vova_buyers db ON fp.buyer_id = db.buyer_id
WHERE
    date( fp.pay_time ) <= '${pre_date}' AND date( fp.pay_time ) > date_sub( '${pre_date}', 7 )
    )
WHERE
    cat_id IS NOT NULL
    AND region_id IS NOT NULL
GROUP BY
    cat_id,
    type,
    region_id,
    gender grouping sets
    ( ( cat_id, type, region_id, gender ),
    ( cat_id, type, gender ) )
    )
INSERT overwrite TABLE ads.ads_vova_category_recommend_default
SELECT
    tmp1.cat_id,
    tmp1.type,
    tmp1.region_id,
    tmp1.gender,
    row_number ( ) over ( PARTITION BY tmp1.region_id, tmp1.gender, tmp1.type ORDER BY nvl ( tmp2.gmv, 0 ) DESC ) rank
FROM
    tmp_all_type tmp1
    LEFT JOIN tmp_gmv tmp2 ON tmp1.cat_id = tmp2.cat_id
    AND tmp1.type = tmp2.type
    AND tmp1.region_id = tmp2.region_id
AND tmp1.gender = tmp2.gender
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=200" \
--conf "spark.app.name=ads_vova_category_recommend_default" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
--conf "spark.sql.crossJoin.enabled=true" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi