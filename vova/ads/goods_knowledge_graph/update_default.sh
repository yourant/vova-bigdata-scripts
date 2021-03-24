#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
insert overwrite table ads.ads_vova_goods_knowledge_graph_default
select
/*+ repartition(1) */
goods_id,
goods_name,
goods_desc,
goods_sn,
first_cat_id,
second_cat_id,
brand_id,
is_on_sale,
is_delete,
sale_vol
from
(select
dg.goods_id,
dg.goods_name,
dg.goods_desc,
dg.goods_sn,
dg2.first_cat_id,
dg2.second_cat_id,
dg.brand_id,
dg.is_on_sale,
dg.is_delete,
sale_vol
from ods_vova_vts.ods_vova_goods dg
inner join dim.dim_vova_goods dg2 on dg.goods_id = dg2.goods_id
left join (select goods_id,sum(goods_number) as sale_vol from dwd.dwd_vova_fact_pay where date(pay_time)<='${pre_date}' and date(pay_time)>date_sub('${pre_date}',90) group by goods_id) sales
on dg.goods_id = sales.goods_id
where  dg2.is_on_sale=1)
where  first_cat_id is not null
"


#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 15G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=ads_vova_goods_knowledge_graph_default" \
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
