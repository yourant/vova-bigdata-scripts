#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
-- req7515
WITH ads_activity_ac_brand_tmp_gooods AS (
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
    cb.sales_vol,
    ab2.brand_name_desc
FROM
    dwd.dwd_vova_activity_goods_ctry_behave cb
    INNER JOIN dim.dim_vova_goods dg ON cb.goods_id = dg.goods_id
    INNER JOIN ads.ads_vova_dictionary_act_brand_v2 ab2 ON dg.brand_id = ab2.brand_id
WHERE
    cb.pt = '${pre_date}'
    AND cb.expre_cnt >= 500
    AND cb.expre_cnt <= 200000
    AND cb.region_id IN ( 0, 4003, 4017 )
    AND cb.clk_cnt/cb.expre_cnt>0.015
    AND cb.ord_cnt/cb.click_uv>0.03
    AND (cb.gmv>=20 OR (brand_name_desc  in ('apple','Cartier','Calvin Klein') and cb.gmv>=10))
    AND ((dg.first_cat_id=5777 AND ab2.brand_name_desc!='apple'  AND cb.gmv / cb.click_uv * cb.clk_cnt / cb.expre_cnt*10000>=75)
    OR ((dg.first_cat_id!=5777 OR ab2.brand_name_desc= 'apple') AND cb.gmv / cb.click_uv * cb.clk_cnt / cb.expre_cnt*10000>=60)
    ))

INSERT overwrite TABLE ads.ads_vova_activity_ac_brand PARTITION ( pt = '${pre_date}' )
SELECT
goods_id,
region_id,
CASE
    WHEN brand_name_desc = 'nike'          THEN 'ac-brand-1'
    WHEN brand_name_desc = 'gucci'         THEN 'ac-brand-2'
    WHEN brand_name_desc = 'moncler'       THEN 'ac-brand-3'
    WHEN brand_name_desc = 'Louis Vuitton' THEN 'ac-brand-4'
    WHEN brand_name_desc = 'apple'         THEN 'ac-brand-5'
    WHEN brand_name_desc = 'dior'          THEN 'ac-brand-6'
    WHEN brand_name_desc = 'chanel'        THEN 'ac-brand-7'
    WHEN brand_name_desc = 'adidas'        THEN 'ac-brand-8'
    WHEN brand_name_desc = 'balenciaga'    THEN 'ac-brand-9'
    WHEN brand_name_desc = 'ugg'           THEN 'ac-brand-10'
    WHEN brand_name_desc = 'Cartier'       THEN 'ac-brand-11'
    WHEN brand_name_desc = 'Calvin Klein'  THEN 'ac-brand-12'
    WHEN brand_name_desc = 'lacoste'       THEN 'ac-brand-13'
    END biz_type,
    3 AS rp_type,
    first_cat_id,
    nvl ( second_cat_id, 0 ) AS second_cat_id,
    row_number ( ) over ( PARTITION BY region_id, brand_name_desc ORDER BY ord_cnt / expre_cnt DESC ) rank
FROM
    ads_activity_ac_brand_tmp_gooods

UNION ALL
SELECT
    goods_id,
    region_id,
    'ac-brand-all' AS biz_type,
    3 AS rp_type,
    first_cat_id,
    nvl ( second_cat_id, 0 ) AS second_cat_id,
    row_number ( ) over ( PARTITION BY region_id, brand_name_desc ORDER BY ord_cnt / expre_cnt DESC ) rank
FROM
ads_activity_ac_brand_tmp_gooods;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_activity_ac_brand" \
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