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
--conf spark.executor.memory=8g \
--conf spark.dynamicAllocation.minExecutors=20 \
--conf spark.dynamicAllocation.maxExecutors=50 \
--conf spark.app.name=user_vova_brand_cat_likes_weight \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.process.UserBrandCatLikesWeight s3://vomkt-emr-rec/jar/vova-bd/dataprocess/new/vova-db-dataprocess-1.0-SNAPSHOT.jar \
--pt ${cur_date}

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi