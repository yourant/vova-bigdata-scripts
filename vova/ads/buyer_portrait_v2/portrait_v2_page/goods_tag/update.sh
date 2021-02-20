#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ads.ads_vova_goods_page_tag partition(gpt)
select
/*+ REPARTITION(3) */
dg.goods_id,
dg.first_cat_name,
dg.second_cat_name,
vb.brand_name,
gp.shop_price,
gp.gs_discount,
gp.shipping_fee,
gp.mct_id,
gp.gmv_1w,
gp.sales_vol_1w,
gp.clk_cnt_1w,
gp.add_cat_cnt_1w,
gp.clk_rate_1w,
gp.pay_rate_1w,
gp.add_cat_rate_1w,
gp.gmv_15d,
gp.sales_vol_15d,
gp.clk_cnt_15d,
gp.add_cat_cnt_15d,
gp.clk_rate_15d,
gp.pay_rate_15d,
gp.add_cat_rate_15d,
gp.gmv_1m,
gp.sales_vol_1m,
gp.clk_cnt_1m,
gp.add_cat_cnt_1m,
gp.clk_rate_1m,
gp.pay_rate_1m,
gp.add_cat_rate_1m,
dg.goods_name,
dg.goods_desc,
gp.comment_cnt_6m,
gp.comment_good_cnt_6m,
gp.comment_bad_cnt_6m,
cast(substr(dg.goods_id,4) as int)%200 as gpt
from
ads.ads_vova_goods_portrait gp
inner join dim.dim_vova_goods dg on gp.gs_id = dg.goods_id
left join ods_vova_vts.ods_vova_brand vb on gp.brand_id = vb.brand_id
where gp.pt='${pre_date}'
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_goods_page_tag" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=200000" \
--conf "spark.network.timeout=300" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
