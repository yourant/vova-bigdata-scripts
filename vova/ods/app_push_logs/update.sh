#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1

#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date: ${cur_date}"

table_suffix=`date -d "${cur_date}" +%Y%m%d`
echo "table_suffix: ${table_suffix}"

job_name="ods_vova_app_push_logs_chenkai_${cur_date}"

###逻辑sql
sql="
msck repair table ods_vova_ext.ods_vova_app_push_logs_raw;

insert overwrite table ods_vova_ext.ods_vova_app_push_logs PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(10) */
install_record_id,
notice_id        ,
platform         ,
user_id          ,
task_id          ,
task_config_id   ,
push_result      ,
response_id      ,
switch_on        ,
push_time
from
(
SELECT
  explode(split(regexp_replace(regexp_replace(data_json, '\\\\[|\\\\]',''),'\\\\}\\\\,\\\\{','\\\\}\\\\;\\\\{'),'\\\\;')) new_json_data
from
  ods_vova_ext.ods_vova_app_push_logs_raw
where
  pt = '${cur_date}'
) a lateral view
json_tuple(new_json_data,
'install_record_id'
,'notice_id'
,'platform'
,'user_id'
,'task_id'
,'task_config_id'
,'push_result'
,'response_id'
,'switch_on'
,'push_time'
) b as install_record_id
,notice_id
,platform
,user_id
,task_id
,task_config_id
,push_result
,response_id
,switch_on
,push_time
;
"

spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=30" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
