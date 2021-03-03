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

job_name="mlb_vova_buyer_activate_time_day180_req8466_chenkai_${cur_date}"

###逻辑sql
sql="
insert overwrite table mlb.mlb_vova_buyer_activate_time_day180 PARTITION (pt = '${cur_date}')
select /*+ REPARTITION(1) */
  buyer_id
from
(
  select
    buyer_id,
    activate_time,
    row_number() over(partition by buyer_id order by activate_time desc) rank
  from
    dim.dim_vova_buyers db
  left join
    dim.dim_vova_devices dd
  on db.current_device_id = dd.device_id
  where to_date(db.last_start_up_date) >= date_sub('${cur_date}', 180)
    and to_date(db.last_start_up_date) <= '${cur_date}'
    and dd.activate_time is not null and db.buyer_id > 0 and db.buyer_id is not null
)
where rank = 1 and buyer_id >= 0
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
