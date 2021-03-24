#!/bin/bash
pt=$1
if [ ! -n "$1" ];then
   pt=`date -d "-1 day" +%Y-%m-%d`
fi
pt_before1=`date -d "${pt} -1 day" +%Y-%m-%d`

shell_dir=$(cd `dirname $0`; pwd)

sed "s/{pt}/${pt}/g; s/{pt_before1}/${pt_before1}/g" ${shell_dir}/mlb_vova_search_intent.sql > ${shell_dir}/tmp_mlb_vova_search_intent.sql
sql=$(cat ${shell_dir}/tmp_mlb_vova_search_intent.sql)
cat ${shell_dir}/tmp_mlb_vova_search_intent.sql

spark-sql --conf "spark.app.name=search_d" \
          --conf "spark.sql.output.merge=true" \
          --conf "spark.sql.output.coalesceNum=50" \
          --conf "spark.dynamicAllocation.maxExecutors=100" \
          --conf "spark.executor.memory=12g" \
          --conf "spark.yarn.maxAppAttempts=1" \
          --conf "spark.executor.memoryOverhead=2048" \
          -e "$sql"
