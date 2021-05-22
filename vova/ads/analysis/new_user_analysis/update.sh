#!/bin/bash
#指定日期和引擎
cur_date=$1
pre_hour=$2
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=$(date "+%Y-%m-%d")
fi
if [ ! -n "$2" ];then
pre_hour=$(date "+%H")
fi

echo "time:${cur_date} ${pre_hour}"

sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ads.ads_vova_new_user_analysis_h partition(pt,hour)
select
count(distinct(if(t2.buyer_id is null ,fp.buyer_id,null))) as new_user_cnt,
count(distinct(t2.buyer_id)) as old_user_cnt,
'${cur_date}' as pt,
'${pre_hour}' as hour
from
dwd.dwd_vova_fact_pay_h fp
left join (select buyer_id from dwd.dwd_vova_fact_pay_h where date(pay_time) < '${cur_date}' group by buyer_id) t2 on fp.buyer_id = t2.buyer_id
where date(fp.pay_time) = '${cur_date}'
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_fact_pay_analysis_h" \
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

if [ $? -ne 0 ];then
  exit 1
fi
