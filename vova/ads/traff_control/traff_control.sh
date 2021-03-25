#!/bin/bash
event_name="traff_control_d"
day=$1
isUpdateDatabase=$2
# 如果day为空取前一天
if [ ! -n "$1" ];then
   day=`date -d "-1 day" +%Y-%m-%d`
   isUpdateDatabase=true
fi
echo "----day: "${day}"; traff_control----"
spark-submit --master yarn \
--deploy-mode client  \
--conf spark.dynamicAllocation.maxExecutors=50 \
--name ads_vova_traffic_control_shudeyou  \
--class com.vova.mct_traff.MctTraff s3://vova-mlb/REC/util/traffic_control.jar ${day} ${isUpdateDatabase}
if [ $? -ne 0 ];then
  echo "${event_name} job error"
  exit 1
fi
