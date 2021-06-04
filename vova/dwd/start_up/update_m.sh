#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 month" +%Y-%m-01`
fi
echo ${cur_date}
###一个月更新一次 fact_start_up_m
sql="
insert overwrite table dwd.dwd_vova_fact_start_up_m partition (month='${cur_date}')
select /*+ REPARTITION(6) */
  datasource    ,
  device_id     ,
  buyer_id      ,
  start_up_date ,
  app_version   ,
  platform      ,
  language_code ,
  region_code   ,
  app_region_code ,
  min_collector_time ,
  max_collector_time ,
  pt
from
  dwd.dwd_vova_fact_start_up
where pt < add_months(trunc('${cur_date}', 'MM'), 1)
  and pt >= '${cur_date}'
;
"

echo "${sql}"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql  --conf "spark.app.name=dwd_vova_fact_start_up_m" --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.sql.output.merge=true"  --conf "spark.sql.output.coalesceNum=1" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi


