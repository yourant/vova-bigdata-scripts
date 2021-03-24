#!/bin/bash
event_name="traff_control_d"
day=$1
isUpdateDatabase=$2
# 如果day为空取前一天
if [ ! -n "$1" ];then
   day=`date -d "-1 day" +%Y/%m/%d/%H`
   isUpdateDatabase=true
fi

echo "----day: "${day}"; traff_control----"
spark-submit --master yarn --deploy-mode client  --conf spark.dynamicAllocation.maxExecutors=50 --name traffic_control  --class com.vova.mct_traff.MctTraff s3://vova-mlb/REC/util/traffic_control.jar ${day} ${isUpdateDatabase}
#spark-submit --master yarn --deploy-mode client  --conf spark.dynamicAllocation.maxExecutors=50 --name traffic_control  --class com.vova.model.Main  s3://vomkt-emr-rec/jar/traffic_control.jar ${day}
#?~B?~^~\?~D~Z?~\?失败?~L?~H~Y?~J??~T~Y
if [ $? -ne 0 ];then
  echo "${event_name} job error"
  exit 1
fi
