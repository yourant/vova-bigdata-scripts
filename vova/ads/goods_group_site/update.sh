#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
-- req7328
WITH tmp_expre AS (
SELECT
    nvl ( fdg.goods_id, -1 ) AS goods_id,
    nvl ( IF ( gi.platform = 'web', 'H5', 'PC' ), 'all' ) AS platform,
    count( * ) AS expre_cnt,
    count( DISTINCT gi.domain_userId ) AS expre_uv
FROM
    dim.dim_zq_goods fdg
    INNER JOIN dwd.dwd_vova_log_goods_impression gi ON fdg.virtual_goods_id = gi.virtual_goods_id
WHERE
    gi.pt <= '${pre_date}' AND gi.pt > date_sub( '${pre_date}', 7 )
    AND gi.platform IN ( 'pc', 'web' )
    AND fdg.goods_id IS NOT NULL
GROUP BY
    fdg.goods_id,
    IF( gi.platform = 'web', 'H5', 'PC' )
    WITH cube
),
tmp_click AS (
SELECT
    nvl ( fdg.goods_id, -1 ) AS goods_id,
    nvl ( IF ( ci.platform = 'web', 'H5', 'PC' ), 'all' ) AS platform,
    count( * ) AS clk_cnt,
    count( DISTINCT ci.domain_userId ) AS clk_uv
FROM
    dim.dim_zq_goods fdg
    INNER JOIN dwd.dwd_vova_log_goods_click ci ON fdg.virtual_goods_id = ci.virtual_goods_id
WHERE
    ci.pt <= '${pre_date}' AND ci.pt > date_sub( '${pre_date}', 7 )
    AND ci.platform IN ( 'pc', 'web' )
    AND fdg.goods_id IS NOT NULL
GROUP BY
    fdg.goods_id,
    IF( ci.platform = 'web', 'H5', 'PC' )
    WITH cube
),
tmp_order AS (
SELECT
    nvl ( og.goods_id, -1 ) AS goods_id,
    nvl ( IF ( oi.from_domain LIKE '%api%', 'H5', 'PC' ), 'all' ) AS platform,
    count( * ) AS order_cnt,
    sum( IF ( oi.pay_status >= 1, og.goods_number, 0 ) ) AS sales_vol,
    count( DISTINCT user_id ) AS order_uv,
    count( DISTINCT IF ( oi.pay_status >= 1, user_id, NULL ) ) AS pay_uv
FROM
    ods_zq_zsp.ods_zq_order_info oi
    INNER JOIN ods_zq_zsp.ods_zq_order_goods og ON og.order_id = oi.order_id
WHERE
    date( oi.pay_time ) <= '${pre_date}' AND date( oi.pay_time ) > date_sub( '${pre_date}', 7 )
    AND og.goods_id IS NOT NULL
GROUP BY
    og.goods_id,
    IF( oi.from_domain LIKE '%api%', 'H5', 'PC' )
    WITH cube
),
tmp_add_cat AS (
SELECT
    nvl ( fdg.goods_id, -1 ) AS goods_id,
    nvl ( IF ( cc.platform = 'web', 'H5', 'PC' ), 'all' ) AS platform,
    count( DISTINCT cc.domain_userId ) AS add_cat_uv
FROM
    dim.dim_zq_goods fdg
    INNER JOIN dwd.dwd_vova_log_common_click cc ON fdg.virtual_goods_id = cc.element_id
WHERE
    cc.pt <= '${pre_date}' AND cc.pt > date_sub( '${pre_date}', 7 )
    AND cc.platform IN ( 'pc', 'web' )
    AND fdg.goods_id IS NOT NULL
GROUP BY
    fdg.goods_id,
    IF( cc.platform = 'web', 'H5', 'PC' )
    WITH cube
)
INSERT overwrite TABLE ads.ads_vova_goods_behave_group_site PARTITION ( pt = '${pre_date}' )
SELECT
    fdg.virtual_goods_id,
    tmp.goods_id,
    platform,
    sum( expre_cnt ) AS expre_cnt,
    sum( clk_cnt ) AS clk_cnt,
    sum( order_cnt ) AS order_cnt,
    sum( sales_vol ) AS sales_vol,
    sum( expre_uv ) AS expre_uv,
    sum( clk_uv ) AS clk_uv,
    sum( add_cat_uv ) AS add_cat_uv,
    sum( order_uv ) AS order_uv,
    sum( pay_uv ) AS pay_uv,
    fdg.commodity_id,
    fdg.datasource as project_name
FROM
    (
SELECT
    goods_id,
    platform,
    expre_cnt,
    0 AS clk_cnt,
    0 AS order_cnt,
    0 AS sales_vol,
    expre_uv,
    0 AS clk_uv,
    0 AS add_cat_uv,
    0 AS order_uv,
    0 AS pay_uv
FROM
    tmp_expre
WHERE
    goods_id > 0

UNION ALL
SELECT
    goods_id,
    platform,
    0 expre_cnt,
    clk_cnt,
    0 AS order_cnt,
    0 AS sales_vol,
    0 AS expre_uv,
    clk_uv,
    0 AS add_cat_uv,
    0 AS order_uv,
    0 AS pay_uv
FROM
    tmp_click
WHERE
    goods_id > 0

UNION ALL
SELECT
    goods_id,
    platform,
    0 AS expre_cnt,
    0 AS clk_cnt,
    order_cnt,
    sales_vol,
    0 AS expre_uv,
    0 AS clk_uv,
    0 AS add_cat_uv,
    order_uv,
    pay_uv
FROM
    tmp_order
WHERE
    goods_id > 0

UNION ALL
SELECT
    goods_id,
    platform,
    0 AS expre_cnt,
    0 AS clk_cnt,
    0 AS order_cnt,
    0 AS sales_vol,
    0 AS expre_uv,
    0 AS clk_uv,
    add_cat_uv,
    0 AS order_uv,
    0 AS pay_uv
FROM
    tmp_add_cat
WHERE
    goods_id > 0
    ) tmp
LEFT JOIN  dim.dim_zq_goods fdg
ON  tmp.goods_id = fdg.goods_id
GROUP BY
    fdg.virtual_goods_id,
    tmp.goods_id,
    platform,
    fdg.commodity_id,
    fdg.datasource

"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=20" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=200" \
--conf "spark.app.name=ads_vova_goods_behave_group_site" \
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