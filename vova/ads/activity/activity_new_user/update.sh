#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
-- req8127
WITH ads_activity_new_user_tmp_gooods AS(
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
    AND cb.first_cat_id in (5712,5715,5769,5768,5777,194)
    AND cb.region_id IN ( 0, 3858, 4003, 4017, 4056, 4143 ))

INSERT OVERWRITE table ads.ads_vova_activity_new_user partition(pt='${pre_date}')
select
goods_id,
region_id,
biz_type,
rp_type,
first_cat_id,
second_cat_id,
rank
from
(select
goods_id,
region_id,
'newuser-jingxuan' AS biz_type,
3 AS rp_type,
first_cat_id,
nvl ( second_cat_id, 0 ) AS second_cat_id,
row_number ( ) over ( PARTITION BY region_id ORDER BY ord_cnt/click_uv DESC ) AS rank
from
ads_activity_new_user_tmp_gooods
where region_id =0
and goods_id in (
    select
    distinct fp.goods_id
    from
    dim.dim_vova_devices dd
    left join dwd.dwd_vova_fact_pay fp
    on fp.order_goods_id = dd.first_order_id
    left join ads.ads_vova_goods_portrait gp
    on fp.goods_id = gp.gs_id and gp.pt='${pre_date}'
    left join dim.dim_vova_goods dg
    on dg.goods_id = fp.goods_id
    where dd.datasource='vova' and datediff('${pre_date}',dd.activate_time)<180
    and (fp.shop_price+fp.shipping_fee)<=3
    and expre_cnt_1w>300
    and dg.first_cat_id in (5712,5715,5769)
    and dg.brand_id=0 and dg.is_on_sale=1
    and sales_vol_1w>1
    )
union all

select
goods_id,
0 as region_id,
'newuser-others' AS biz_type,
3 AS rp_type,
first_cat_id,
nvl ( second_cat_id, 0 ) AS second_cat_id,
row_number ( ) over ( PARTITION BY first_cat_id ORDER BY gmv / click_uv * clk_cnt / expre_cnt DESC  ) AS rank
from
ads_activity_new_user_tmp_gooods
where
region_id =0 and
goods_id in (
    select
    distinct fp.goods_id
    from
    dim.dim_vova_devices dd
    left join dwd.dwd_vova_fact_pay fp
    on fp.order_goods_id = dd.first_order_id
    left join ads.ads_vova_goods_portrait gp
    on fp.goods_id = gp.gs_id and gp.pt='${pre_date}'
    left join dim.dim_vova_goods dg
    on dg.goods_id = fp.goods_id
    where dd.datasource='vova' and datediff('${pre_date}',dd.activate_time)<180
  --  and (fp.shop_price+fp.shipping_fee)<=3
    and expre_cnt_1w>300
    and dg.first_cat_id in (5712,5715,5769,5768,5777,194)
    and dg.brand_id=0 and dg.is_on_sale=1
    and sales_vol_1w>1
    ))
where rank<=200;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_activity_new_user" \
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