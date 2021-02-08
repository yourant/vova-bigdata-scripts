#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  cur_date=`date +%Y-%m-%d`
fi

sql="
insert overwrite table ads.ads_vova_buyer_brand_level
select
db.buyer_id,
if(tmp.rank<=100000,1,0) as is_brand
from
dim.dim_vova_buyers db
left join
(select
buyer_id,
row_number() over(order by sum(1*0+2*gb.add_cat_cnt+5*gb.ord_cnt) desc) rank
from
dws.dws_vova_buyer_goods_behave gb
inner join dim.dim_vova_goods dg
on gb.gs_id = dg.goods_id
where pt>date_sub('${cur_date}',180)
and pt<='${cur_date}'
and dg.brand_id>0
and buyer_id>0
group by
buyer_id)tmp
on db.buyer_id = tmp.buyer_id
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=ads_vova_buyer_brand_level" \
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