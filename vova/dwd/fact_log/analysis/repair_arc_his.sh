#!/bin/bash

stime=$1
etime=$2
#默认日期为昨天
if [ ! -n "$1" ];then
stime=`date -d "-1 hour" +%Y/%m/%d/%H`
fi
if [ ! -n "$2" ];then
etime=`date +%Y/%m/%d/%H`
fi

echo "
spark-submit --master yarn \
--deploy-mode client \
--conf spark.executor.memory=6g \
--conf spark.dynamicAllocation.maxExecutors=150 \
--conf spark.app.name=FactLogToHiveOffline_${stime}_${etime} \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.bigdata.sparkbatch.dataprocess.snowplow.job.FactLogToHiveOffline \
s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar \
--envFile prod --partitionNum 300 --startTime ${stime} --endTime ${etime} --handle_warehouse false
"

spark-submit --master yarn \
--deploy-mode client \
--conf spark.executor.memory=6g \
--conf spark.dynamicAllocation.maxExecutors=100 \
--conf spark.app.name=FactLogToHiveOffline_${stime}_${etime} \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.bigdata.sparkbatch.dataprocess.snowplow.job.FactLogToHiveOffline \
s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar \
--envFile prod --partitionNum 300 --startTime ${stime} --endTime ${etime} --handle_warehouse false

if [ $? -ne 0 ];then
  exit 1
fi
