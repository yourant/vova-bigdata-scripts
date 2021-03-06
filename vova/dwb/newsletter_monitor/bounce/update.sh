#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
insert overwrite table rpt.rpt_newsletter_monitor_bounce PARTITION ( pt = '${pre_date}' )
select
nvl(app_name,'all') as app_name,
max(value*100) bounce_rate_max,
avg(value*100) bounce_rate_avg
from
ods.vova_prometheus
where pt='${pre_date}'
and type = 'aws_ses_reputation_bounce_rate_average'
group by
app_name
with cube
"


#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 15G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=rpt_newsletter_monitor_bounce" \
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

