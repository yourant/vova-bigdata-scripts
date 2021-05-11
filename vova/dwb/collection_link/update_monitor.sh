#!/bin/bash
cur_date=$1
if [ ! -n "$1" ];then
cur_date=`date -d "-2 day" +%Y-%m-%d`
fi

spark-submit \
--deploy-mode client \
--conf spark.dynamicAllocation.maxExecutors=30 \
--conf spark.default.parallelism=380 \
--conf spark.sql.shuffle.partitions=380 \
--conf spark.sql.adaptive.enabled=true \
--conf spark.sql.adaptive.join.enabled=true \
--conf spark.shuffle.sort.bypassMergeThreshold=10000 \
--conf spark.sql.inMemoryColumnarStorage.compressed=true \
--conf spark.sql.inMemoryColumnarStorage.partitionPruning=true \
--conf spark.sql.inMemoryColumnarStorage.batchSize=100000 \
--conf spark.network.timeout=300 \
--conf spark.app.name=vova_collection_link_monitor \
--conf spark.executor.memoryOverhead=2048 \
--conf spark.eventLog.enabled=false \
--driver-java-options "-Dlog4j.configuration=hdfs:/conf/log4j.properties" \
--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=hdfs:/conf/log4j.properties" \
--class com.vova.bigdata.sparkbatch.dataprocess.dwb.CollelctionRate s3://vomkt-emr-rec/jar/vova-bigdata-collection-rate.jar \
--cur_date ${cur_date} --knock_array Juntao,Eason.Chen,Lengshan,Eric.Yu,Zhangxin,Mic.Fang,Motong

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
