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
if [ $? -ne 0 ];then
   exit 1
fi

spark-sql --conf "spark.app.name=mlb_vova_search_query_d_shudeyou" \
          --conf "spark.dynamicAllocation.maxExecutors=100" \
          --conf "spark.executor.memory=8g" \
          -e "$sql"
#if error
if [ $? -ne 0 ];then
   exit 1
fi