#!/bin/bash
# 取数jar包位置：s3://vova-mlb/REC/model/nbi/nbi-1.0-SNAPSHOT.jar
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

###依赖的表：dws.dws_vova_buyer_goods_behave, dim.dim_vova_goods
spark-submit --name nbi_v2_hel_chenkai \
--master yarn --deploy-mode cluster \
--driver-memory 8G \
--executor-memory 8G \
--num-executors 100 \
--conf spark.akka.frameSize=1024 \
--conf spark.yarn.executor.memoryOverhead=5120 \
--conf spark.sql.broadcastTimeout=-1 \
--conf spark.dynamicAllocation.maxExecutors=120 \
--conf spark.driver.maxResultSize=10G \
--conf spark.storage.blockManagerTimeoutIntervalMs=10000 \
--conf spark.default.parallelism=1000 \
--conf spark.shuffle.sort.bypassMergeThreshold=10000 \
--conf spark.sql.inMemoryColumnarStorage.batchSize=100000 \
--conf spark.memory.storageFraction=0.2 \
--conf spark.memory.fraction=0.8 \
--conf spark.sql.shuffle.partitions=500 \
--conf spark.dynamicAllocation.enabled=true \
--class com.vova.nbi \
s3://vova-mlb/REC/model/nbi/nbi-1.0-SNAPSHOT.jar 20 200

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`