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

echo "${stime}， ${etime}"

spark-submit \
--master yarn \
--deploy-mode cluster \
--conf spark.executor.memory=10g \
--conf spark.dynamicAllocation.maxExecutors=200 \
--conf spark.yarn.maxAppAttempts=1 \
--conf spark.app.name=dwb_vova_recall_pool_${stime}_${etime}_req_chenkai \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.bigdata.sparkbatch.dataprocess.dwb.RecallPool s3://vomkt-emr-rec/jar/dwb-vova-recall-pool/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar \
--startTime ${stime} --endTime ${etime}



