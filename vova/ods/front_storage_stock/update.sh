#!/bin/bash
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

spark-submit --master yarn   \
--conf spark.executor.memory=10g \
--conf spark.dynamicAllocation.maxExecutors=110 \
--conf spark.yarn.maxAppAttempts=1 \
--conf spark.sql.autoBroadcastJoinThreshold=-1 \
--conf spark.app.name=vova-bigdata-ods-front_storage_stock \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.bigdata.sparkbatch.dataprocess.ods.FrontStorageStock s3://vomkt-emr-rec/jar/vova-bigdata-ods-front_storage_stock.jar \
--envFile prod

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi