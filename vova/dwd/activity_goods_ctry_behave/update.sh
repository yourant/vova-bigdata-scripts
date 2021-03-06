#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
insert overwrite table dwd.dwd_vova_activity_goods_ctry_behave partition(pt='${pre_date}')
SELECT
    tmp1.goods_id,
    dg.first_cat_id,
    dg.second_cat_id,
    tmp1.region_id,
    if(dg.brand_id=0,0,1) as is_brand,
    tmp1.expre_cnt,
    tmp1.clk_cnt,
    tmp1.ord_cnt,
    tmp1.gmv,
    tmp1.users AS expre_uv,
    tmp1.click_uv,
    tmp1.sales_vol
FROM
    (
SELECT
    gb.gs_id AS goods_id,
    nvl ( db.region_id, 0 ) AS region_id,
    sum( gb.expre_cnt ) AS expre_cnt,
    sum( gb.clk_cnt ) AS clk_cnt,
    sum( gb.ord_cnt ) AS ord_cnt,
    sum( gb.gmv ) AS gmv,
    sum( gb.sales_vol ) AS sales_vol,
    count( DISTINCT IF ( gb.expre_cnt > 0, gb.buyer_id, NULL ) ) AS users,
    count( DISTINCT IF ( gb.clk_cnt > 0, gb.buyer_id, NULL ) ) AS click_uv
FROM
    dws.dws_vova_buyer_goods_behave gb
    LEFT JOIN dim.dim_vova_buyers db ON db.buyer_id = gb.buyer_id
WHERE
    gb.pt <= '${pre_date}' AND gb.pt > date_sub( '${pre_date}', 7 )
    AND db.region_id IS NOT NULL AND db.region_id != 0
GROUP BY
    gb.gs_id,
    db.region_id grouping sets ( ( gb.gs_id, db.region_id ), ( gb.gs_id ) )
    ) tmp1
    LEFT JOIN (
SELECT
    fp.goods_id,
    count(
IF
    (
    fr.order_goods_id IS NOT NULL
    AND fr.refund_reason_type_id != 8
    AND fr.refund_type_id = 2
    AND fr.exec_refund_time >= fp.pay_time,
    order_id,
NULL
    )
    ) / count( 1 ) AS refund_rate
FROM
    dwd.dwd_vova_fact_pay fp
    LEFT JOIN dwd.dwd_vova_fact_refund fr ON fp.order_goods_id = fr.order_goods_id
WHERE
    date( fp.order_time ) > date_sub( '${pre_date}', 7 )
    AND date( fp.order_time ) <= '${pre_date}'
GROUP BY
    fp.goods_id
    ) tmp_refund ON tmp1.goods_id = tmp_refund.goods_id
    INNER JOIN dim.dim_vova_goods dg ON tmp1.goods_id = dg.goods_id
    INNER JOIN ads.ads_vova_mct_rank mr on dg.mct_id = mr.mct_id and dg.first_cat_id = mr.first_cat_id and mr.pt='${pre_date}'
WHERE
    ( refund_rate < 0.15 OR refund_rate IS NULL )
    AND mr.rank>=3
    -- AND tmp1.region_id IN ( 0, 3858, 4003, 4017, 4056, 4143 );
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--driver-memory 10G  \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=200" \
--conf "spark.app.name=dwd_vova_activity_goods_ctry_behave" \
--conf "spark.default.parallelism = 580" \
--conf "spark.sql.shuffle.partitions=580" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=500000" \
--conf "spark.network.timeout=300" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi