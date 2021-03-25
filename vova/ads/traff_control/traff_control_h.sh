#!/bin/bash
event_name="traff_control_h"
pt=$1
hour=$2
isUpdateDatabase=$3

if [ ! -n "$1" ];then
pt=`date "+%Y-%m-%d"`
hour=`date "+%H"`
isUpdateDatabase=true
fi

echo "pt=$pt"
echo "hour=$hour"

spark-submit --master yarn \
 --driver-memory 2G  \
 --num-executors 10 \
 --name ads_vova_traffic_control_h_shudeyou \
 --conf spark.dynamicAllocation.enabled=false \
 --conf spark.driver.memoryOverhead=2048 \
 --conf spark.executor.memoryOverhead=2048 \
 --conf spark.default.parallelism=300 \
 --conf spark.sql.shuffle.partitions=300 \
 --conf spark.sql.session.timeZone=UTC  \
 --class com.vova.mct_traff.UpdateProbPage \
 s3://vova-mlb/REC/util/traffic_control.jar ${pt} ${hour} ${isUpdateDatabase}

if [ $? -ne 0 ];then
  echo "${event_name} job error"
  exit 1
fi
