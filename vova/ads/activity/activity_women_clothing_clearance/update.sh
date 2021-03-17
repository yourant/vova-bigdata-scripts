#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
-- req7343
WITH ads_activity_women_clothing_clearance_sale_tmp_gooods AS (
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
    if(tmp_coupon_value.goods_id is null,0,1) is_act
FROM
    dwd.dwd_vova_activity_goods_ctry_behave cb
LEFT join
(
  -- 折扣比例
  select
    goods_id,
    max(coupon_value) coupon_value
  from
    ods_vova_vts.ods_vova_activity_coupon
  where activity_id =415523 and coupon_type = 'percent' and coupon_value>=0.2
  group by goods_id
) tmp_coupon_value on tmp_coupon_value.goods_id = cb.goods_id
WHERE
    cb.pt='${pre_date}'
    AND cb.first_cat_id = 194
    AND cb.expre_cnt >= 500
    AND clk_cnt / expre_cnt >= 0.02
    AND ((tmp_coupon_value.goods_id is not null and gmv / click_uv * clk_cnt / expre_cnt * 10000 >= 40)
    OR (tmp_coupon_value.goods_id is null and gmv / click_uv * clk_cnt / expre_cnt * 10000 >= 60))
    AND cb.region_id IN ( 0, 3858, 4003, 4017, 4056, 4143 )),

tmp_replace as(
SELECT
goods_id,
region_id,
biz_type,
rp_type,
is_add,
gmv,
row_number ( ) over ( PARTITION BY region_id, biz_type, rp_type, goods_id ORDER BY is_add ,gmv DESC ) grank
from
(SELECT
nvl ( tmp2.min_price_goods_id, tmp1.goods_id ) AS goods_id,
tmp1.region_id,
tmp1.biz_type,
tmp1.rp_type,
tmp1.is_add,
tmp1.gmv
from
(select
    t1.goods_id,
    t1.region_id,
    'Jan-womensclothing' as biz_type,
    3 as rp_type,
    t1.gmv,
    0 as is_add
from
    ads_activity_women_clothing_clearance_sale_tmp_gooods t1

union all
SELECT
    t1.goods_id,
    explode ( split ( '3858,4003,4017,4056,4143', ',' ) ) AS region_id,
    'Jan-womensclothing' as biz_type,
    3 as rp_type,
    t1.gmv,
    1 as is_add
FROM
    ads_activity_women_clothing_clearance_sale_tmp_gooods t1
WHERE
    t1.region_id = 0) tmp1
LEFT JOIN dim.dim_vova_goods dg on tmp1.goods_id = dg.goods_id
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
            AND dg.second_cat_id = tmp2.second_cat_id))
insert overwrite table ads.ads_vova_activity_women_clothing_clearance_sale partition(pt='${pre_date}')
select
*
from
(select
   t1.goods_id,
   t1.region_id,
   t1.biz_type,
    t1.rp_type,
    dg.first_cat_id,
    nvl(dg.second_cat_id,0) as second_cat_id,
    row_number ( ) over ( PARTITION BY t1.region_id, t1.biz_type, t1.rp_type ORDER BY is_add,t1.gmv DESC ) rank
from
tmp_replace t1
left join dim.dim_vova_goods dg on t1.goods_id = dg.goods_id
where t1.grank = 1)
where rank<=500;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_activity_clearance_sale" \
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