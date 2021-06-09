#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
WITH ads_vova_activity_low_price_goods_tmp AS (
SELECT
  cb.goods_id,
  cb.region_id,
  cb.first_cat_id,
  cb.second_cat_id,
  dg.shop_price + dg.shipping_fee AS goods_price,
  nvl(cb.ord_cnt / cb.expre_cnt, 0) AS cr
FROM
  dwd.dwd_vova_activity_goods_ctry_behave cb
    inner join dim.dim_vova_goods dg on dg.goods_id = cb.goods_id
WHERE cb.pt='${pre_date}'
  AND cb.expre_cnt >= 500
  AND cb.clk_cnt/cb.expre_cnt>0.02
  AND cb.gmv / cb.click_uv * cb.clk_cnt / cb.expre_cnt * 10000 >= 40 -- gcr
  AND cb.region_id IN (0, 3858, 4003, 4017, 4056, 4143)
  AND dg.shop_price + dg.shipping_fee <= 24
  AND dg.brand_id = 0),

temp1 (
select
min_price_goods_id,
region_id,
first_cat_id,
second_cat_id,
cr
from
(
select
min_price_goods_id,
region_id,
tmp3.first_cat_id,
tmp3.second_cat_id,
cr,
row_number() over(PARTITION BY min_price_goods_id, region_id ORDER BY cr DESC) grank
from
(
select
nvl(tmp2.min_price_goods_id, t1.goods_id) AS min_price_goods_id,
t1.region_id,
t1.first_cat_id,
t1.second_cat_id,
t1.cr
from
ads_vova_activity_low_price_goods_tmp t1
    LEFT JOIN
    (
      SELECT
        mpg.goods_id,
        mpg.min_price_goods_id
      FROM
        ads.ads_vova_min_price_goods_d mpg
      WHERE pt = '${pre_date}'
        AND strategy = 'c'
    ) tmp2 on t1.goods_id = tmp2.goods_id
WHERE t1.goods_price > 6 AND t1.goods_price <= 24
) tmp3
inner join dim.dim_vova_goods dg on dg.goods_id = tmp3.min_price_goods_id
where dg.brand_id = 0
AND dg.shop_price + dg.shipping_fee > 6
AND dg.shop_price + dg.shipping_fee <= 24
) tmp4
where tmp4.grank = 1
),


temp2 (
select
min_price_goods_id,
region_id,
first_cat_id,
second_cat_id,
cr
from
(
select
min_price_goods_id,
region_id,
tmp3.first_cat_id,
tmp3.second_cat_id,
cr,
row_number() over(PARTITION BY min_price_goods_id, region_id ORDER BY cr DESC) grank
from
(
select
nvl(tmp2.min_price_goods_id, t1.goods_id) AS min_price_goods_id,
t1.region_id,
t1.first_cat_id,
t1.second_cat_id,
t1.cr
from
ads_vova_activity_low_price_goods_tmp t1
    LEFT JOIN
    (
      SELECT
        mpg.goods_id,
        mpg.min_price_goods_id
      FROM
        ads.ads_vova_min_price_goods_d mpg
      WHERE pt = '${pre_date}'
        AND strategy = 'c'
    ) tmp2 on t1.goods_id = tmp2.goods_id
WHERE t1.goods_price <= 6
) tmp3
inner join dim.dim_vova_goods dg on dg.goods_id = tmp3.min_price_goods_id
where dg.brand_id = 0
AND dg.shop_price + dg.shipping_fee <= 6
) tmp4
where tmp4.grank = 1
)

INSERT overwrite TABLE ads.ads_vova_activity_low_price_goods PARTITION (pt = '${pre_date}')
select
/*+ REPARTITION(1) */
*
from
(SELECT
    min_price_goods_id AS goods_id,
    region_id,
    'high-value-goods' AS biz_type,
    3 AS rp_type,
    first_cat_id,
    nvl(second_cat_id, 0) AS second_cat_id,
    row_number () over ( PARTITION BY region_id ORDER BY cr DESC ) rank
FROM
temp1)
where rank <= 500

UNION ALL

select
/*+ REPARTITION(1) */
*
from
(SELECT
    min_price_goods_id AS goods_id,
    region_id,
    'low-value-goods' AS biz_type,
    3 AS rp_type,
    first_cat_id,
    nvl(second_cat_id, 0) AS second_cat_id,
    row_number () over ( PARTITION BY region_id ORDER BY cr DESC ) rank
FROM
temp2)
where rank <= 500
;


WITH temp3 (
select
min_price_goods_id,
region_id,
first_cat_id,
second_cat_id,
cr
from
(
select
min_price_goods_id,
region_id,
tmp3.first_cat_id,
tmp3.second_cat_id,
cr,
row_number() over(PARTITION BY min_price_goods_id, region_id ORDER BY cr DESC) grank
from
(
select
nvl(tmp2.min_price_goods_id, cb.goods_id) AS min_price_goods_id,
cb.region_id,
cb.first_cat_id,
cb.second_cat_id,
nvl(cb.ord_cnt / cb.expre_cnt, 0) AS cr
from
dwd.dwd_vova_activity_goods_ctry_behave cb
    inner join dim.dim_vova_goods dg on dg.goods_id = cb.goods_id
    LEFT JOIN
    (
      SELECT
        mpg.goods_id,
        mpg.min_price_goods_id
      FROM
        ads.ads_vova_min_price_goods_d mpg
      WHERE pt = '${pre_date}'
        AND strategy = 'c'
    ) tmp2 on cb.goods_id = tmp2.goods_id
WHERE cb.pt='${pre_date}'
  AND cb.clk_cnt/cb.expre_cnt>0
  AND cb.region_id IN (0, 3858, 4003, 4017, 4056, 4143)
  AND dg.shop_price + dg.shipping_fee <= 24
  AND dg.brand_id = 0
) tmp3
inner join dim.dim_vova_goods dg on dg.goods_id = tmp3.min_price_goods_id
where dg.brand_id = 0
AND dg.shop_price + dg.shipping_fee <= 24
) tmp4
where tmp4.grank = 1
)

INSERT overwrite TABLE ads.ads_vova_activity_newly_activated_goods PARTITION (pt = '${pre_date}')
select
/*+ REPARTITION(1) */
*
from
(SELECT
    min_price_goods_id AS goods_id,
    region_id,
    'newly-activated-products' AS biz_type,
    3 AS rp_type,
    first_cat_id,
    nvl(second_cat_id, 0) AS second_cat_id,
    row_number () over ( PARTITION BY region_id ORDER BY cr DESC ) rank
FROM
temp3)
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_activity_low_price_goods" \
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
