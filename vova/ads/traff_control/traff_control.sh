#!/bin/bash
#指定日期和引擎
event_name="traff_control"
day=$1
#默认日期为昨天
if [ ! -n "$1" ];then
day=`date -d "-1 day" +%Y/%m/%d/%H`
fi

echo "----day: "${day}"; traff_control----"
spark-submit --master yarn \
--conf spark.app.name=ads_traffic_control_shudeyou \
--class com.vova.model_new.Main \
s3://vova-mlb/REC/util/traffic_control.jar ${day}

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  echo "${event_name} job error"
  exit 1
fi