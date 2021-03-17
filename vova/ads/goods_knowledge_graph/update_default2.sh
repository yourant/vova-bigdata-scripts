#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
insert overwrite table ads.ads_vova_goods_knowledge_graph_default_all
select
/*+ repartition(10) */
goods_id,
goods_name,
goods_desc,
goods_sn,
first_cat_id,
second_cat_id,
brand_id,
is_on_sale,
is_delete
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
row_number() over(partition by dg2.first_cat_id order by nvl(ord_cnt_1m,0) desc,nvl(expre_cnt_1m,0) desc ) rank
from ods_vova_vts.ods_vova_goods dg
inner join dim.dim_vova_goods dg2 on dg.goods_id = dg2.goods_id
left join  ads.ads_vova_goods_portrait gp
on gp.gs_id = dg.goods_id
where gp.pt='2021-03-08' and dg2.is_on_sale=1)
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
