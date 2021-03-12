#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi


hive -e "msck repair table ods_vova_ext.ods_vova_vvqueue_vvevent_arc;"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
spark-sql   --conf "spark.app.name=ods_vova_vvqueue_vvevent" --conf "spark.sql.crossJoin.enabled=true"  --conf "spark.dynamicAllocation.maxExecutors=120"  -e "
insert overwrite table ods_vova_ext.ods_vova_vvqueue_vvevent PARTITION(pt = '${cur_date}')
select json_tuple(json_str, 'project', 'plat_form', 'event_type', 'event_fingerprint', 'device_id', 'uid','language', 'time', 'extra')
from ods_vova_ext.ods_vova_vvqueue_vvevent_arc where pt = '${cur_date}'

"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi