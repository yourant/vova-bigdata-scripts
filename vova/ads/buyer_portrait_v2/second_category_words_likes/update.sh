#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

spark-submit \
--deploy-mode client \
--master yarn  \
--driver-memory 4G \
--conf spark.dynamicAllocation.minExecutors=20 \
--conf spark.dynamicAllocation.initExecutors=20 \
--conf spark.dynamicAllocation.maxExecutors=200 \
--conf spark.default.parallelism=380 \
--conf spark.sql.shuffle.partitions=380 \
--conf spark.sql.adaptive.enabled=true \
--conf spark.sql.adaptive.join.enabled=true \
--conf spark.shuffle.sort.bypassMergeThreshold=10000 \
--conf spark.sql.inMemoryColumnarStorage.compressed=true \
--conf spark.sql.inMemoryColumnarStorage.partitionPruning=true \
--conf spark.sql.inMemoryColumnarStorage.batchSize=100000 \
--conf spark.network.timeout=300 \
--conf spark.app.name=GoodsKeyWordIDF \
--conf spark.executor.memoryOverhead=2048 \
--conf spark.eventLog.enabled=false \
--driver-java-options "-Dlog4j.configuration=hdfs:/conf/log4j.properties" \
--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=hdfs:/conf/log4j.properties" \
--class com.vova.process.GoodsKeyWordIDF s3://vomkt-emr-rec/jar/vova-bd/dataprocess/vova-db-dataprocess-1.0-SNAPSHOT-new.jar \
--env prod \
--pt "${pre_date}"