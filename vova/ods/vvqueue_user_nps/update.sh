#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo ${cur_date}

hive -e "msck repair table ods_vova_ext.ods_vova_vvqueue_user_nps_arc;"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sql="
insert overwrite table ods_vova_ext.ods_vova_vvqueue_user_nps PARTITION(pt = '${cur_date}')
select
c0 as user_email,
cast(c1 as bigint) as user_id,
c2 as reasons,
c3 as reason_text,
cast(c4 as timestamp) as add_time,
cast(c5 as int) as score
from
(select json_tuple(json_str, 'user_email', 'user_id', 'reasons', 'reason_text', 'add_time', 'score')
from ods_vova_ext.ods_vova_vvqueue_user_nps_arc where pt =  '${cur_date}')
"

spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ods_vova_vvqueue_user_nps" \
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