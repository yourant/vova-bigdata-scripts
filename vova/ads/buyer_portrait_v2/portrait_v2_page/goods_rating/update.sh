#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ads.ads_vova_buyer_page_rating_top  partition(bpt)
select
/*+ REPARTITION(3) */
tmp1.buyer_id,
db.datasource,
db.current_device_id,
db.email,
tmp1.goods_id,
vg.goods_thumb,
tmp1.rating,
cast(substr(tmp1.buyer_id,4) as int)%200 as bpt
from
(select
gr.user_id as buyer_id,
gr.goods_id,
gr.rating,
row_number() over(partition by gr.user_id order by gr.rating desc) rk
from
ads.ads_vova_buyer_goods_rating gr
where pt='${pre_date}')tmp1
left join ods_vova_vts.ods_vova_goods vg on vg.goods_id = tmp1.goods_id
left join dim.dim_vova_buyers db on db.buyer_id = tmp1.buyer_id
where rk <=30
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=200" \
--conf "spark.app.name=ads_vova_buyer_page_rating_top" \
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
