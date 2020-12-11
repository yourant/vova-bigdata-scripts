#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###更新用户首单
sql="
drop table if exists ods_vova_vts.ods_vova_user_wallet_part;
create table if not exists ods_vova_vts.ods_vova_user_wallet_part as
select *
from ods_vova_vts.ods_vova_user_wallet_part_0
union
select *
from ods_vova_vts.ods_vova_user_wallet_part_1
union
select *
from ods_vova_vts.ods_vova_user_wallet_part_2
union
select *
from ods_vova_vts.ods_vova_user_wallet_part_3
union
select *
from ods_vova_vts.ods_vova_user_wallet_part_4
union
select *
from ods_vova_vts.ods_vova_user_wallet_part_5
union
select *
from ods_vova_vts.ods_vova_user_wallet_part_6
union
select *
from ods_vova_vts.ods_vova_user_wallet_part_7
union
select *
from ods_vova_vts.ods_vova_user_wallet_part_8
union
select *
from ods_vova_vts.ods_vova_user_wallet_part_9
union
select *
from ods_vova_vts.ods_vova_user_wallet_part_10
union
select *
from ods_vova_vts.ods_vova_user_wallet_part_11
union
select *
from ods_vova_vts.ods_vova_user_wallet_part_12
union
select *
from ods_vova_vts.ods_vova_user_wallet_part_13
union
select *
from ods_vova_vts.ods_vova_user_wallet_part_14
union
select *
from ods_vova_vts.ods_vova_user_wallet_part_15;
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.dynamicAllocation.maxExecutors=100"   --conf "spark.dynamicAllocation.minExecutors=20"  --conf "spark.sql.parquet.writeLegacyFormat=true" --conf "spark.app.name=merge_vova_user_wallet_part" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
