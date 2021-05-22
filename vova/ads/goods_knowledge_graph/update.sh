#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
insert overwrite table ads.ads_vova_goods_knowledge_graph partition(pt='${pre_date}')
select
/*+ repartition(1) */
gs_id as goods_id,
dg.goods_name,
dg.goods_desc,
dg.goods_sn,
gp.first_cat_id,
gp.second_cat_id,
dg.brand_id,
dg.is_on_sale,
dg.is_delete
from ads.ads_vova_goods_portrait gp
inner join ods_vova_vts.ods_vova_goods dg
on gp.gs_id = dg.goods_id
where date(cast(dg.add_time as timestamp)) = '${pre_date}' and gp.pt = '${pre_date}'
;
"


#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 15G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=ads_vova_goods_knowledge_graph" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.autoBroadcastJoinThreshold=-1" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
