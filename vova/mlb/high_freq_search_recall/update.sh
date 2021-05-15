#!/bin/bash
#高频搜索词
spark-submit --master yarn \
--deploy-mode cluster \
--driver-cores 2 \
--driver-memory 10G \
--executor-cores 1 \
--executor-memory 8G \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.driver.memoryOverhead=1024 \
--conf spark.executor.memoryOverhead=2048 \
--conf spark.default.parallelism=1000 \
--conf spark.sql.shuffle.partitions=1000 \
--conf spark.driver.maxResultSize=12G \
--conf spark.dynamicAllocation.maxExecutors=100 \
--name high-freq-data --conf spark.yarn.maxAppAttempts=3 \
--class com.vova.model.Main s3://vomkt-emr-rec/jar/vova-mlb-high-freq-search.jar

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
#语义识别
spark-submit --master yarn \
--deploy-mode cluster \
--driver-cores 2 \
--driver-memory 10G \
--executor-cores 1 \
--executor-memory 8G \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.driver.memoryOverhead=1024 \
--conf spark.executor.memoryOverhead=2048 \
--conf spark.default.parallelism=1000 \
--conf spark.sql.shuffle.partitions=1000 \
--conf spark.driver.maxResultSize=12G \
--conf spark.dynamicAllocation.maxExecutors=100 \
--name search-ebr-recall-data --conf spark.yarn.maxAppAttempts=3 \
--class com.vova.model.Main s3://vomkt-emr-rec/jar/vova-mlb-ebr.jar

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi