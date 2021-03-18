#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
-- req7343
WITH ads_activity_women_cloth_tmp_gooods AS (
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
    cb.pt='${pre_date}'
    AND cb.first_cat_id = 194
    AND cb.is_brand = 0
    AND cb.expre_cnt >= 500
    AND cb.region_id IN ( 0, 3858, 4003, 4017, 4056, 4143 )),
tmp_second_cat AS (
SELECT
    goods_id,
    region_id,
    'nvzhuang-coats' AS biz_type,
    57 AS rp_type,
    gmv
FROM
    ads_activity_women_cloth_tmp_gooods tmp1
WHERE
    second_cat_id = 171
    AND clk_cnt / expre_cnt >= 0.01
    AND gmv >= 20
    AND ord_cnt / click_uv >= 0.01
    AND tmp1.gmv / tmp1.click_uv * tmp1.clk_cnt / tmp1.expre_cnt * 10000 >= 60

UNION ALL
SELECT
    goods_id,
    region_id,
    'nvzhuang-sweatshirts' AS biz_type,
    57 AS rp_type,
    gmv
FROM
    ads_activity_women_cloth_tmp_gooods tmp1
WHERE
    second_cat_id = 5962
    AND clk_cnt / expre_cnt >= 0.015
    AND ord_cnt / click_uv >= 0.02
    AND tmp1.gmv / tmp1.click_uv * tmp1.clk_cnt / tmp1.expre_cnt * 10000 >= 60

UNION ALL
SELECT
    goods_id,
    region_id,
    'nvzhuang-pants' AS biz_type,
    57 AS rp_type,
    gmv
FROM
    ads_activity_women_cloth_tmp_gooods tmp1
WHERE
    second_cat_id = 5963
    AND clk_cnt / expre_cnt >= 0.015
    AND ord_cnt / click_uv >= 0.02
    AND tmp1.gmv / tmp1.click_uv * tmp1.clk_cnt / tmp1.expre_cnt * 10000 >= 60

UNION ALL
SELECT
    goods_id,
    region_id,
    'nvzhuang-dresses' AS biz_type,
    57 AS rp_type,
    gmv
FROM
    ads_activity_women_cloth_tmp_gooods tmp1
WHERE
    second_cat_id = 195
    AND clk_cnt / expre_cnt >= 0.015
    AND ord_cnt / click_uv >= 0.02
    AND tmp1.gmv / tmp1.click_uv * tmp1.clk_cnt / tmp1.expre_cnt * 10000 >= 60

UNION ALL
SELECT
    goods_id,
    region_id,
    'nvzhuang-pullover' AS biz_type,
    57 AS rp_type,
    gmv
FROM
    ads_activity_women_cloth_tmp_gooods tmp1
WHERE
    second_cat_id = 3004
    AND clk_cnt / expre_cnt >= 0.015 -- and gmv>=50

    AND ord_cnt / click_uv >= 0.02
    AND tmp1.gmv / tmp1.click_uv * tmp1.clk_cnt / tmp1.expre_cnt * 10000 >= 40

UNION ALL
SELECT
    goods_id,
    region_id,
    'nvzhuang-blousestshirts' AS biz_type,
    57 AS rp_type,
    gmv
FROM
    ads_activity_women_cloth_tmp_gooods tmp1
WHERE
    second_cat_id IN ( 164, 165 )
    AND clk_cnt / expre_cnt >= 0.015 -- and gmv>=50

    AND ord_cnt / click_uv >= 0.02
    AND tmp1.gmv / tmp1.click_uv * tmp1.clk_cnt / tmp1.expre_cnt * 10000 >= 60

UNION ALL
SELECT
    goods_id,
    region_id,
    'nvzhuang-sleep' AS biz_type,
    57 AS rp_type,
    gmv
FROM
    ads_activity_women_cloth_tmp_gooods tmp1
WHERE
    second_cat_id = 5930
    AND clk_cnt / expre_cnt >= 0.01 -- and gmv>=50

    AND ord_cnt / click_uv >= 0.02
    AND tmp1.gmv / tmp1.click_uv * tmp1.clk_cnt / tmp1.expre_cnt * 10000 >= 40

UNION ALL
SELECT
    goods_id,
    region_id,
    'nvzhuang-socks' AS biz_type,
    57 AS rp_type,
    gmv
FROM
    ads_activity_women_cloth_tmp_gooods tmp1
WHERE
    second_cat_id = 5928
    AND clk_cnt / expre_cnt >= 0.01 -- and gmv>=50

    AND ord_cnt / click_uv >= 0.01
    AND tmp1.gmv / tmp1.click_uv * tmp1.clk_cnt / tmp1.expre_cnt * 10000 >= 40

UNION ALL
SELECT
    goods_id,
    region_id,
    'nvzhuang-swimwear' AS biz_type,
    57 AS rp_type,
    gmv
FROM
    ads_activity_women_cloth_tmp_gooods tmp1
WHERE
    second_cat_id = 3001
    AND clk_cnt / expre_cnt >= 0.015 -- and gmv>=50
    AND ord_cnt / click_uv >= 0.02
    -- AND tmp1.gmv / tmp1.click_uv * tmp1.clk_cnt / tmp1.expre_cnt * 10000 >= 40
    ),
    tmp_second_cat_add (
    SELECT
        goods_id,
        region_id,
        biz_type,
        rp_type,
        row_number ( ) over ( PARTITION BY region_id, biz_type ORDER BY is_add, gmv DESC ) rank
    FROM
        (
        SELECT
            goods_id,
            region_id,
            biz_type,
            rp_type,
            gmv,
            is_add,
            row_number ( ) over ( PARTITION BY goods_id, region_id, biz_type ORDER BY is_add, gmv DESC ) rank
        FROM
            (
            SELECT
                goods_id,
                region_id,
                biz_type,
                rp_type,
                gmv,
                0 AS is_add
            FROM
                tmp_second_cat t1
    UNION ALL
            SELECT
                goods_id,
                region_id,
                biz_type,
                rp_type,
                gmv,
                1 AS is_add
            FROM
                (
                SELECT
                    t1.goods_id,
                    explode ( split ( '3858,4003,4017,4056,4143', ',' ) ) AS region_id,
                    t1.biz_type,
                    t1.rp_type,
                    t1.gmv
                FROM
                    tmp_second_cat t1
                WHERE
                    t1.region_id = 0
                )
            )
        )
    WHERE
        rank = 1
    ),
    tmp_bs_57 (
    SELECT
        tmp1.goods_id,
        tmp1.region_id,
        'nvzhuang-bestsellers' AS biz_type,
        57 AS rp_type,
        row_number ( ) over ( PARTITION BY tmp1.region_id ORDER BY tmp1.gmv DESC ) rank
    FROM
        ads_activity_women_cloth_tmp_gooods tmp1
    WHERE
        (
            ( tmp1.second_cat_id = 171 AND tmp1.clk_cnt / tmp1.expre_cnt >= 0.01 )
            OR tmp1.clk_cnt / tmp1.expre_cnt >= 0.015
        )
        AND (
            ( tmp1.second_cat_id = 171 AND tmp1.ord_cnt / tmp1.click_uv >= 0.01 )
            OR tmp1.ord_cnt / tmp1.click_uv >= 0.03
        )
        AND tmp1.gmv / tmp1.click_uv * tmp1.clk_cnt / tmp1.expre_cnt * 10000 >= 60
        AND EXISTS ( SELECT 1 FROM ads_activity_women_cloth_tmp_gooods tg WHERE tg.region_id = 0 AND tmp1.goods_id = tg.goods_id AND tg.gmv >= 60 )
    ),
    tmp_bs_7 (
    SELECT
        tmp1.goods_id,
        tmp1.region_id,
        tmp1.biz_type,
        7 AS rp_type,
        row_number ( ) over ( PARTITION BY tmp1.region_id, tmp1.biz_type ORDER BY ads_activity_women_cloth_tmp_gooods.ord_cnt DESC ) rank
    FROM
        tmp_bs_57 tmp1
        LEFT JOIN ads_activity_women_cloth_tmp_gooods ON tmp1.goods_id = ads_activity_women_cloth_tmp_gooods.goods_id
        AND tmp1.region_id = ads_activity_women_cloth_tmp_gooods.region_id
    ),
    tmp_new (
    SELECT
        tmp1.goods_id,
        tmp1.region_id,
        'nvzhuang-newarrival' biz_type,
        57 AS rp_type,
        row_number ( ) over ( PARTITION BY tmp1.region_id ORDER BY dg.first_on_time DESC ) rank
    FROM
        ads_activity_women_cloth_tmp_gooods tmp1
        INNER JOIN dim.dim_vova_goods dg ON tmp1.goods_id = dg.goods_id
    WHERE
        tmp1.expre_cnt >= 100
        AND tmp1.expre_cnt <= 100000 AND tmp1.clk_cnt / tmp1.expre_cnt >= 0.015
        AND tmp1.ord_cnt / tmp1.click_uv >= 0.025
        AND tmp1.gmv >= 1
        AND NOT EXISTS ( SELECT 1 FROM tmp_second_cat_add ca WHERE tmp1.goods_id = ca.goods_id )
        AND NOT EXISTS ( SELECT 1 FROM tmp_bs_57 ca WHERE tmp1.goods_id = ca.goods_id )
        AND NOT EXISTS ( SELECT 1 FROM tmp_bs_7 ca WHERE tmp1.goods_id = ca.goods_id )
    ),
    tmp_all AS (
    SELECT
        *
    FROM
        tmp_second_cat_add
    WHERE
        rank <= 500

UNION ALL
    SELECT
        *
    FROM
        tmp_bs_57
    WHERE
        rank <= 600

UNION ALL
    SELECT
        *
    FROM
        tmp_bs_7
    WHERE
        rank <= 100

UNION ALL
    SELECT
        *
    FROM
        tmp_new
    WHERE
        rank <= 600
    ),
tmp_all_rep(
SELECT
    t1.goods_id,
    t1.region_id,
    t1.biz_type,
    t1.rp_type,
    dg.first_cat_id,
    nvl(dg.second_cat_id,0) as second_cat_id,
    row_number ( ) over ( PARTITION BY t1.region_id, t1.biz_type, t1.rp_type ORDER BY t1.rank DESC ) rank
FROM
    (
    SELECT
        goods_id,
        region_id,
        biz_type,
        rp_type,
        row_number ( ) over ( PARTITION BY region_id, biz_type, rp_type, goods_id ORDER BY rank DESC ) grank,
        rank
    FROM
        (
        SELECT
            nvl ( tmp2.min_price_goods_id, tmp1.goods_id ) AS goods_id,
            tmp1.region_id,
            tmp1.biz_type,
            tmp1.rp_type,
            tmp1.rank,
        IF
            ( tmp2.min_price_goods_id IS NOT NULL, 1, 0 ) REPLACE
        FROM
            tmp_all tmp1
            LEFT JOIN dim.dim_vova_goods dg ON tmp1.goods_id = dg.goods_id
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
            AND dg.second_cat_id = tmp2.second_cat_id
        )
    )t1
left join dim.dim_vova_goods dg
on t1.goods_id = dg.goods_id
WHERE
grank = 1
)
INSERT overwrite TABLE ads.ads_vova_activity_women_cloth partition(pt='${pre_date}')
SELECT
*
FROM
tmp_all_rep t1
WHERE not exists (select 1 from tmp_all_rep t2 where t1.biz_type = 'nvzhuang-newarrival' and t2.biz_type!='nvzhuang-newarrival' and t1.goods_id = t2.goods_id)
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_activity_women_cloth" \
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