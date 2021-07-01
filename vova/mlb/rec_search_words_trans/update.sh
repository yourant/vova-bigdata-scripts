#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
hive -e "msck repair table tmp.tmp_vova_search_words_trans_result_json;"
spark-sql \
--executor-memory 8G --executor-cores 1 \
--driver-memory 8g \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=rec_search_words_trans" \
--conf "spark.default.parallelism = 430" \
--conf "spark.sql.shuffle.partitions=430" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
--conf "spark.sql.crossJoin.enabled=true" \
-e "
insert overwrite table tmp.tmp_vova_search_words_trans_result  PARTITION (pt = '${cur_date}')
select json_tuple(json, 'source', 'result')
from (SELECT explode(split(regexp_replace(regexp_replace(regexp_replace(regexp_replace(trans_result,'\\\\, ','\\\\,'),': ',':'),'\\\\[|\\\\]',''),'\\\\}\\\\,\\\\{','\\\\}\\\\;\\\\{'),'\\\\;')) as json from tmp.tmp_vova_search_words_trans_result_json where pt = '${cur_date}') test
;

insert overwrite table mlb.mlb_vova_user_query_translation_d_new  PARTITION (pt = '${cur_date}')
select /*+ REPARTITION(1) */
nvl(b.source,a.source),
nvl(b.result,a.result)
from tmp.tmp_vova_search_words_trans_result a
left join tmp.tmp_vova_search_words_trans_result_800 b on a.source = b.source
left join mlb.mlb_vova_user_query_translation_d_new c on a.source = c.clk_from
where a.pt = '${cur_date}' and a.source is not null
and c.clk_from is null
group by nvl(b.source,a.source),nvl(b.result,a.result)
"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
