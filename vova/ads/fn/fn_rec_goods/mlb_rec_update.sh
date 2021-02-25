#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "$cur_date"

spark-submit \
--master yarn  \
--name fn_mlb_rec \
--deploy-mode cluster \
--driver-cores 2 \
--driver-memory 10G \
--executor-cores 1 \
--executor-memory 15G \
--conf spark.hadoop.mapred.output.compress=false \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.driver.memoryOverhead=1024 \
--conf spark.executor.memoryOverhead=2048 \
--conf spark.default.parallelism=1000 \
--conf spark.sql.shuffle.partitions=1000 \
--conf spark.driver.maxResultSize=12G \
--conf spark.dynamicAllocation.maxExecutors=100 \
--conf spark.yarn.maxAppAttempts=1 \
--class com.vova.rec.model.item2vec.Main s3://vova-mlb/FN-REC/util/rec-training.jar $cur_date
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

