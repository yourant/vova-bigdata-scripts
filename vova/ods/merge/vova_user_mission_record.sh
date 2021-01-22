#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###更新用户首单
sql="
insert overwrite table ods_vova_vts.ods_vova_user_mission_record
select *
from ods_vova_vts.ods_vova_user_mission_record_0
union
select *
from ods_vova_vts.ods_vova_user_mission_record_1
union
select *
from ods_vova_vts.ods_vova_user_mission_record_2
union
select *
from ods_vova_vts.ods_vova_user_mission_record_3
union
select *
from ods_vova_vts.ods_vova_user_mission_record_4
union
select *
from ods_vova_vts.ods_vova_user_mission_record_5
union
select *
from ods_vova_vts.ods_vova_user_mission_record_6
union
select *
from ods_vova_vts.ods_vova_user_mission_record_7
union
select *
from ods_vova_vts.ods_vova_user_mission_record_8
union
select *
from ods_vova_vts.ods_vova_user_mission_record_9
union
select *
from ods_vova_vts.ods_vova_user_mission_record_10
union
select *
from ods_vova_vts.ods_vova_user_mission_record_11
union
select *
from ods_vova_vts.ods_vova_user_mission_record_12
union
select *
from ods_vova_vts.ods_vova_user_mission_record_13
union
select *
from ods_vova_vts.ods_vova_user_mission_record_14
union
select *
from ods_vova_vts.ods_vova_user_mission_record_15;
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.dynamicAllocation.maxExecutors=100"   --conf "spark.dynamicAllocation.minExecutors=20"  --conf "spark.sql.parquet.writeLegacyFormat=true" --conf "spark.app.name=merge_vova_user_mission_record" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
