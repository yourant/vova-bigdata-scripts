#!/bin/bash
cur_date=$1
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
now_date=`date -d "-1 days ago ${cur_date}" +%Y-%m-%d`

spark-submit \
--deploy-mode client \
--master yarn  \
--driver-memory 8G \
--conf spark.executor.memory=8g \
--conf spark.dynamicAllocation.maxExecutors=120 \
--conf spark.default.parallelism=380 \
--conf spark.sql.shuffle.partitions=380 \
--conf spark.sql.adaptive.enabled=true \
--conf spark.sql.adaptive.join.enabled=true \
--conf spark.shuffle.sort.bypassMergeThreshold=10000 \
--conf spark.sql.inMemoryColumnarStorage.compressed=true \
--conf spark.sql.inMemoryColumnarStorage.partitionPruning=true \
--conf spark.sql.inMemoryColumnarStorage.batchSize=100000 \
--conf spark.network.timeout=300 \
--conf spark.app.name=rec_vova_user_clk_behave_link_d \
--conf spark.executor.memoryOverhead=2048 \
--conf spark.eventLog.enabled=false \
--driver-java-options "-Dlog4j.configuration=hdfs:/conf/log4j.properties" \
--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=hdfs:/conf/log4j.properties" \
--class com.vova.bigdata.sparkbatch.dataprocess.ads.GoodsClickCause s3://vomkt-emr-rec/jar/vova-goods-click-cause-v2.jar \
--pt ${cur_date} --now_date ${now_date}

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
