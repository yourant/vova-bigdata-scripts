#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1

#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date: ${cur_date}"

job_name="dwd_vova_fact_log_bonus_card"

###逻辑sql
sql="
msck repair table ods.vova_bonus_card_status;

insert OVERWRITE TABLE dwd.dwd_vova_fact_log_bonus_card PARTITION (pt='${cur_date}')
select
/*+ REPARTITION(1) */
  bonus_card_id,
  user_id,
  old_status,
  new_status,
  update_time
from
(
  select
    CAST(get_json_object(data, '$.bonus_card_id') AS BIGINT) bonus_card_id,
    CAST(get_json_object(data, '$.user_id') AS BIGINT) user_id,
    get_json_object(data, '$.old_status') old_status,
    get_json_object(data, '$.new_status') new_status,
    get_json_object(data, '$.update_time') update_time,
    from_unixtime(get_json_object(data, '$.update_time'), 'yyyy-MM-dd') pt
  from
  ods.vova_bonus_card_status
  where pt >= date_sub('${cur_date}', 1) and pt <= date_sub('${cur_date}', -1)
)
where pt = '${cur_date}'
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
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

