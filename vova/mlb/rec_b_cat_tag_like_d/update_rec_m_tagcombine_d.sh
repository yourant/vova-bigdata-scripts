#!/bin/bash
# mlb.mlb_rec_m_tagcombine_d
# 取数jar包位置：s3://vova-mlb/REC_TEST/hel/hot_cat_rec/spark_task/hot_cat.jar
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

###依赖的表：mlb.mlb_vova_user_behave_link_d，ads.ads_vova_goods_attribute_label_data
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
--name mlb_hot_cat_tagcombine_heliu_chenkai \
--conf spark.yarn.maxAppAttempts=1 \
--class com.vova.hot_cat_kg \
s3://vova-mlb/REC_TEST/hel/hot_cat_rec/spark_task/hot_cat.jar 30 1 2

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
echo "end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

