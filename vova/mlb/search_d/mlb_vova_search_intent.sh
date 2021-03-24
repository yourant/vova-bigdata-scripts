#!/bin/bash
pt=$1
if [ ! -n "$1" ];then
   pt=`date -d "-1 day" +%Y-%m-%d`
fi
pt_before30=`date -d "${pt} -30 day" +%Y-%m-%d`

shell_dir=$(cd `dirname $0`; pwd)

sed "s/{pt}/${pt}/g; s/{pt_before30}/${pt_before30}/g" ${shell_dir}/mlb_vova_search_intent.sql > ${shell_dir}/tmp_mlb_vova_search_intent.sql
sql=$(cat ${shell_dir}/tmp_mlb_vova_search_intent.sql)
cat ${shell_dir}/tmp_mlb_vova_search_intent.sql

spark-sql --conf "spark.app.name=search_d" \
          --conf "spark.dynamicAllocation.maxExecutors=100" \
          --conf "spark.executor.memory=8g" \
          -e "$sql"
if [ $? -ne 0 ];then
   exit 1
fi