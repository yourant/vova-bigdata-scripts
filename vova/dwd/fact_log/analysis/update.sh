#!/bin/bash
#指定日期和引擎
event_name="goods_click"
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
fi

echo "
spark-submit --master yarn \
--deploy-mode client \
--conf spark.executor.memory=15g \
--conf spark.dynamicAllocation.maxExecutors=200 \
--conf spark.app.name=AnalysisLogToHiveOffline_${pt} \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.bigdata.sparkbatch.dataprocess.snowplow.job.AnalysisLogToHiveOffline \
s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar \
--envFile prod --pt ${pt}
"
spark-submit --master yarn \
--deploy-mode client \
--conf spark.executor.memory=15g \
--conf spark.dynamicAllocation.maxExecutors=200 \
--conf spark.app.name=AnalysisLogToHiveOffline_${pt} \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.bigdata.sparkbatch.dataprocess.snowplow.job.AnalysisLogToHiveOffline \
s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar \
--envFile prod --pt ${pt}

if [ $? -ne 0 ];then
  exit 1
fi

