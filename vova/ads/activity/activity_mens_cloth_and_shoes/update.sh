#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
-- req7515
WITH ads_activity_men_cloth_and_shoes_tmp_gooods AS(
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
    cb.sales_vol
FROM
    dwd.dwd_vova_activity_goods_ctry_behave cb
WHERE
    cb.pt = '${pre_date}'
    AND cb.is_brand = 0
    AND cb.expre_cnt >= 100
    AND cb.region_id IN ( 0, 3858, 4003, 4017, 4056, 4143 )
    AND cb.first_cat_id in (5768,5777)),
 tmp_gcr_ctr AS (
SELECT
    tmp1.goods_id,
    tmp1.first_cat_id,
    tmp1.second_cat_id,
    tmp1.region_id,
    tmp1.expre_cnt,
    tmp1.clk_cnt,
    tmp1.ord_cnt,
    tmp1.gmv,
    tmp1.expre_uv,
    tmp1.click_uv,
    tmp1.sales_vol
FROM
    ads_activity_men_cloth_and_shoes_tmp_gooods tmp1
WHERE
    EXISTS ( SELECT 1 FROM ads_activity_men_cloth_and_shoes_tmp_gooods tmp2 WHERE tmp2.region_id = 0  AND tmp2.goods_id = tmp1.goods_id AND tmp2.gmv >= 20 )
    AND tmp1.gmv / tmp1.click_uv * tmp1.clk_cnt / tmp1.expre_cnt * 10000 >= 60
    AND tmp1.clk_cnt / tmp1.expre_cnt > 0.015
    ),
    tmp_cloth_hot AS (
SELECT
    goods_id,
    region_id,
    'menscloting-hot' AS biz_type,
    3 AS rp_type,
    first_cat_id,
    second_cat_id,
    row_number ( ) over ( PARTITION BY region_id ORDER BY gmv DESC ) AS gmv_rank
FROM
    tmp_gcr_ctr
WHERE
    first_cat_id = 5768
    AND expre_cnt >= 500
    AND ord_cnt / click_uv >= 0.02
    ),
    tmp_jacket_coats AS (
SELECT
    goods_id,
    region_id,
    'menscloting-jacketscoats' AS biz_type,
    3 AS rp_type,
    first_cat_id,
    second_cat_id,
    row_number ( ) over ( PARTITION BY region_id ORDER BY gmv DESC ) AS gmv_rank
FROM
    tmp_gcr_ctr
WHERE
    first_cat_id = 5768
    AND second_cat_id = 5785
    AND expre_cnt >= 500
    AND ord_cnt / click_uv >= 0.015
    ),
    tmp_hoodies_sweatshirts (
SELECT
    goods_id,
    region_id,
    'menscloting-hoodiessweatshirts' AS biz_type,
    3 AS rp_type,
    first_cat_id,
    second_cat_id,
    row_number ( ) over ( PARTITION BY region_id ORDER BY gmv DESC ) AS gmv_rank
FROM
    tmp_gcr_ctr
WHERE
    first_cat_id = 5768
    AND second_cat_id = 5795
    AND expre_cnt >= 500
    AND ord_cnt / click_uv >= 0.015
    ),
    tmp_cloth_more (
SELECT
    goods_id,
    region_id,
    'menscloting-more' AS biz_type,
    3 AS rp_type,
    first_cat_id,
    second_cat_id,
    row_number ( ) over ( PARTITION BY region_id ORDER BY gmv DESC ) AS gmv_rank
FROM
    tmp_gcr_ctr
WHERE
    first_cat_id = 5768
    AND expre_cnt >= 500
    AND ord_cnt / click_uv >= 0.01
    ),
    tmp_shoes_hot (
SELECT
    goods_id,
    region_id,
    'shoes-hot' AS biz_type,
    3 AS rp_type,
    first_cat_id,
    second_cat_id,
    row_number ( ) over ( PARTITION BY region_id ORDER BY gmv DESC ) AS gmv_rank
FROM
    tmp_gcr_ctr
WHERE
    first_cat_id = 5777
    AND expre_cnt >= 500
    AND ord_cnt / click_uv >= 0.02
    ),
    tmp_shoes_athleticshoes (
SELECT
    goods_id,
    region_id,
    'shoes-athleticshoes' AS biz_type,
    3 AS rp_type,
    first_cat_id,
    second_cat_id,
    row_number ( ) over ( PARTITION BY region_id ORDER BY gmv DESC ) AS gmv_rank
FROM
    tmp_gcr_ctr
WHERE
    first_cat_id = 5777
    AND second_cat_id = 5883
    AND expre_cnt >= 500
    AND ord_cnt / click_uv >= 0.015
    ),
    tmp_shoes_boots (
SELECT
    goods_id,
    region_id,
    'shoes-boots' AS biz_type,
    3 AS rp_type,
    first_cat_id,
    second_cat_id,
    row_number ( ) over ( PARTITION BY region_id ORDER BY gmv DESC ) AS time_rank
FROM
    tmp_gcr_ctr
WHERE
    first_cat_id = 5777
    AND second_cat_id = 5968
    AND expre_cnt >= 500
    AND ord_cnt / click_uv >= 0.015
    ),
    tmp_shoes_more (
SELECT
    goods_id,
    region_id,
    'shoes-more' AS biz_type,
    3 AS rp_type,
    first_cat_id,
    second_cat_id,
    row_number ( ) over ( PARTITION BY region_id ORDER BY gmv DESC ) AS time_rank
FROM
    tmp_gcr_ctr
WHERE
    first_cat_id = 5777
    AND expre_cnt >= 500
    AND ord_cnt / click_uv >= 0.01
    ),
    tmp_all (
SELECT
    nvl ( tmp2.goods_id, tmp1.goods_id ) AS goods_id,
    tmp1.region_id,
    tmp1.biz_type,
    tmp1.rp_type,
    tmp1.first_cat_id,
    tmp1.second_cat_id,
    tmp1.gmv_rank AS rank,
    row_number ( ) over ( PARTITION BY tmp1.biz_type, tmp1.rp_type, tmp1.region_id, tmp1.goods_id ORDER BY tmp1.gmv_rank ) drank
FROM
    (
    SELECT
     *
    FROM
    tmp_cloth_hot

    UNION ALL
    SELECT
    *
    FROM
    tmp_jacket_coats

    UNION ALL
    SELECT
     *
    FROM
    tmp_hoodies_sweatshirts

    UNION ALL
    SELECT
     *
    FROM
    tmp_cloth_more

    UNION ALL
    SELECT
     *
    FROM
    tmp_shoes_hot

    UNION ALL
    SELECT
     *
    FROM
    tmp_shoes_athleticshoes

UNION ALL
    SELECT
     *
    FROM
    tmp_shoes_boots

    UNION ALL
    SELECT
     *
    FROM
    tmp_shoes_more
    ) tmp1
    LEFT JOIN (
SELECT
    mpg.goods_id,
    mpg.min_price_goods_id,
    dg.second_cat_id
FROM
    ads.ads_vova_min_price_goods_h mpg
    LEFT JOIN dim.dim_vova_goods dg ON mpg.min_price_goods_id = dg.goods_id
WHERE
    pt = '${pre_date}'
    AND strategy = 'c'
    ) tmp2 ON tmp1.goods_id = tmp2.goods_id
    AND tmp1.second_cat_id = tmp2.second_cat_id
    ),
    tmp_cloth_new (
SELECT
    tmp1.goods_id,
    tmp1.region_id,
    'menscloting-new' AS biz_type,
    3 AS rp_type,
    tmp1.first_cat_id,
    tmp1.second_cat_id,
    row_number ( ) over ( PARTITION BY tmp1.region_id ORDER BY dg.first_on_time DESC ) rank
FROM
    ads_activity_men_cloth_and_shoes_tmp_gooods tmp1
    INNER JOIN dim.dim_vova_goods dg ON tmp1.goods_id = dg.goods_id
WHERE
    tmp1.first_cat_id = 5768
    AND expre_cnt >= 100
    AND expre_cnt <= 100000 AND clk_cnt / expre_cnt >= 0.015
    AND ord_cnt / click_uv >= 0.02
    ),
    tmp_shoes_new (
SELECT
    tmp1.goods_id,
    tmp1.region_id,
    'shoes-new' AS biz_type,
    3 AS rp_type,
    tmp1.first_cat_id,
    tmp1.second_cat_id,
    row_number ( ) over ( PARTITION BY tmp1.region_id ORDER BY dg.first_on_time DESC ) rank
FROM
    ads_activity_men_cloth_and_shoes_tmp_gooods tmp1
    INNER JOIN dim.dim_vova_goods dg ON tmp1.goods_id = dg.goods_id
WHERE
    tmp1.first_cat_id = 5777
    AND expre_cnt >= 100
    AND expre_cnt <= 100000 AND clk_cnt / expre_cnt >= 0.015
    AND ord_cnt / click_uv >= 0.02
    ),
    tmp_new (
SELECT
    goods_id,
    region_id,
    biz_type,
    rp_type,
    first_cat_id,
    second_cat_id,
    rank,
    drank
    from
    (
SELECT
    nvl ( tmp2.goods_id, tmp1.goods_id ) AS goods_id,
    tmp1.region_id,
    tmp1.biz_type,
    tmp1.rp_type,
    tmp1.first_cat_id,
    tmp1.second_cat_id,
    tmp1.rank,
    row_number ( ) over ( PARTITION BY tmp1.biz_type, tmp1.rp_type, tmp1.region_id, tmp1.goods_id ORDER BY tmp1.rank ) drank
FROM
    ( SELECT * FROM tmp_cloth_new UNION ALL SELECT * FROM tmp_shoes_new ) tmp1
    LEFT JOIN (
SELECT
    mpg.goods_id,
    mpg.min_price_goods_id,
    dg.second_cat_id
FROM
    ads.ads_vova_min_price_goods_h mpg
    LEFT JOIN dim.dim_vova_goods dg ON mpg.min_price_goods_id = dg.goods_id
WHERE
    pt = '${pre_date}'
    AND strategy = 'c'
    ) tmp2 ON tmp1.goods_id = tmp2.goods_id
    AND tmp1.second_cat_id = tmp2.second_cat_id
    ) tmp1
WHERE
    NOT EXISTS ( SELECT 1 FROM tmp_all WHERE tmp_all.drank = 1 AND tmp1.goods_id = tmp_all.goods_id )
    )

    INSERT overwrite TABLE ads.ads_vova_activity_men_cloth_and_shoes partition(pt='${pre_date}')
    SELECT
    goods_id,
    region_id,
    biz_type,
    rp_type,
    first_cat_id,
    nvl(second_cat_id,0) as second_cat_id,
    row_number ( ) over ( PARTITION BY region_id, biz_type, rp_type ORDER BY rank DESC ) rank
FROM
    (
SELECT
    goods_id,
    region_id,
    biz_type,
    rp_type,
    first_cat_id,
    second_cat_id,
    rank
FROM
    tmp_all
WHERE
    drank = 1 UNION ALL
SELECT
    goods_id,
    region_id,
    biz_type,
    rp_type,
    first_cat_id,
    second_cat_id,
    rank
FROM
    tmp_new
WHERE
    drank = 1
    )
WHERE  (biz_type='menscloting-hot' AND  rank<=100) or (biz_type!='menscloting-hot' AND rank<=300)

"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_activity_men_cloth_and_shoes" \
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