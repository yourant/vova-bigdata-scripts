#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

sql="
insert overwrite table ads.ads_vova_search_gender_rate partition(pt='${cur_date}')
select
key_word,
sum(male_search_count) as male_search_count,
sum(female_search_count) as female_search_count
from
(select
sw.key_word,
sum(if(db.gender='male',1,0)) male_search_count,
sum(if(db.gender='female',1,0)) female_search_count
from
dwd.dwd_vova_fact_search_word sw
inner join dim.dim_vova_buyers db
on sw.buyer_id = db.buyer_id
where  sw.pt='${cur_date}' and  sw.datasource='vova' and db.gender in ('male','female')
group by
sw.key_word
union all

select
key_word,
male_search_count,
female_search_count
from
ads.ads_vova_search_gender_rate
where pt=date_sub('${cur_date}',1))
group by key_word
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_search_gender_rate" \
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
if [ $? -ne 0 ];then
  exit 1
fi

