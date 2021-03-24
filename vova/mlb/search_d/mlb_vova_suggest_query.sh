#!/bin/bash
pt=$1
if [ ! -n "$1" ];then
   pt=`date -d "-1 day" +%Y-%m-%d`
fi
pt_before60=`date -d "${pt} -60 day" +%Y-%m-%d`

shell_dir=$(cd `dirname $0`; pwd)


sed "s/{pt}/${pt}/g; s/{pt_before60}/${pt_before60}/g" ${shell_dir}/mlb_vova_suggest_query.sql > ${shell_dir}/tmp_mlb_vova_suggest_query.sql
sql=$(cat ${shell_dir}/tmp_mlb_vova_suggest_query.sql)
cat ${shell_dir}/tmp_mlb_vova_suggest_query.sql


spark-sql --conf "spark.app.name=search_d" \
          --conf "spark.sql.output.merge=true" \
          --conf "spark.sql.output.coalesceNum=20" \
          --conf "spark.dynamicAllocation.maxExecutors=100" \
          --conf "spark.executor.memory=12g" \
          --conf "spark.yarn.maxAppAttempts=1" \
          --conf "spark.executor.memoryOverhead=2048" \
          -e "$sql"
#if error
if [ $? -ne 0 ];then
   exit 1
fi