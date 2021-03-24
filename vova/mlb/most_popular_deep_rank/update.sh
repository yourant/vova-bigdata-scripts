#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#如果使用spark-sql运行，则执行spark-sql -e
spark-submit --master yarn \
--deploy-mode cluster \
--driver-cores 2 \
--driver-memory 8G \
--executor-cores 1 \
--executor-memory 8G \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.hadoop.mapred.output.compress=false \
--conf spark.driver.memoryOverhead=1024 \
--conf spark.executor.memoryOverhead=2048 \
--conf spark.default.parallelism=1000 \
--conf spark.sql.shuffle.partitions=1000 \
--conf spark.driver.maxResultSize=12G \
--conf spark.dynamicAllocation.maxExecutors=100 \
--name mlb_vova_deep_rank_req8970_heliu_chenkai \
--conf spark.yarn.maxAppAttempts=1 \
--class com.vova.most_popular s3://vova-mlb/REC/data/rank/most_popular/spark_task/spark_utils-1.0-SNAPSHOT.jar 60 0 10 1

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

echo "end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
