#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ads.ads_vova_goods_comment partition(gpt)
select
/*+ COALESCE(1) */
fc.goods_id,
fc.comment_id,
fc.rating,
fc.comment,
upper(dl.languages_code) lan_code,
fc.post_time,
fc.tag,
cast(substr(fc.goods_id,4) as int)%200 as gpt
from
dwd.dwd_vova_fact_comment fc
left join dim.dim_vova_languages dl on fc.language_id = dl.languages_id
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_goods_comment" \
--conf "spark.default.parallelism=380" \
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
