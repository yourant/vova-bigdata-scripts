#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

spark-submit \
--deploy-mode client \
--master yarn  \
--num-executors 3 \
--executor-cores 1 \
--executor-memory 8G \
--driver-memory 8G \
--conf spark.app.name=vovaHighRefundGoodsMonitor \
--conf spark.executor.memoryOverhead=2048 \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.eventLog.enabled=false \
--driver-java-options "-Dlog4j.configuration=hdfs:/conf/log4j.properties" \
--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=hdfs:/conf/log4j.properties" \
--class com.vova.process.SendData2Interface s3://vomkt-emr-rec/jar/vova-bd/dataprocess/new/vova-db-dataprocess-1.0-SNAPSHOT.jar \
--env prod \
--sql "select goods_id,web_order_rate,web_order_rate_ratio,expre_efficiency,expre_efficiency_ratio,new_user_rate,new_user_rate_ratio,new_user_order_rate,new_user_order_ratio from dwb.dwb_vova_conversion_monitor where pt='${cur_date}' " \
--url "http://vvfeature.vova.com.hk/api/v1/abnormal/high-refund-goods" \
--secretKey  "69b562h101b7kdbac2066ls10b26d02b" \
--batchSize 3000 \
--id vovaOrderMonitorTest
