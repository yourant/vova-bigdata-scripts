#!/bin/sh
home=`dirname "$0"`
cd $home

table="dwb_fd_realtime_rpt_comparison"
user="lujiaheng"

if [ "$#" -ne 3 ]; then
  pt=$(date -d "$ts -1 hours" +"%Y-%m-%d")
  hour=$(date -d "$ts -1 hours" +"%H")
else
  pt=$2
  hour=$3
fi

#hive sql中使用的变量
echo $pt

#脚本路径
shell_path="/mnt/vova-bigdata-scripts/fd/dwb/${table}"

spark-sql \
--conf "spark.app.name=${table}_${user}"   \
--conf "spark.dynamicAllocation.maxExecutors=60" \
-d pt=$pt \
-d hour=$hour \
-f ${shell_path}/insert.hql


#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${table}  table is finished !"

