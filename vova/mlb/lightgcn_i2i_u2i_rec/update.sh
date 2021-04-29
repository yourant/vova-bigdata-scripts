#!/bin/bash
# 取数jar包位置：s3://vova-mlb/REC/util/lightgcn.jar
echo "start_time:" $(date +"%Y-%m-%d %H:%M:%S" -d "8 hour")

###依赖的表：数据来源
#      dim.dim_vova_goods
#      dim.dim_vova_buyers
#      dws.dws_vova_buyer_goods_behave
#      ads.ads_vova_goods_portrait

spark-submit \
--master yarn \
--deploy-mode cluster \
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
--name mlb_lightgcn_i2i_u2i_rec_gongrui_chenkai \
--conf spark.yarn.maxAppAttempts=1 \
--class com.vova.model.lightgcn \
s3://vova-mlb/REC/util/lightgcn.jar 15
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
echo "end_time:" $(date +"%Y-%m-%d %H:%M:%S" -d "8 hour")
