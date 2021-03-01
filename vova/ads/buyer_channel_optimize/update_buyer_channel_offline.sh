#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
#离线更新一次
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "cur_date: $cur_date"

job_name="ads_vova_buyer_channel_req8523_chenkai_${cur_date}"

#
sql="
insert overwrite table ads.ads_vova_buyer_channel partition(pt='${cur_date}')
select /*+ REPARTITION(6) */
  device_id,
  main_channel,
  child_channel,
  channel
from
(
  select
    trim(dd.device_id) device_id,
    dd.main_channel main_channel,
    dd.child_channel child_channel,
    dd.child_channel channel,
    row_number() over(partition by trim(dd.device_id) order by activate_time desc) row
  from
    dim.dim_vova_devices dd
  where dd.datasource = 'vova'
    and trim(dd.device_id) is not null
    and trim(dd.device_id) != ''
    and dd.main_channel is not null and dd.child_channel is not null
    and dd.main_channel != '' and dd.child_channel !=''
) where row = 1
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
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
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
