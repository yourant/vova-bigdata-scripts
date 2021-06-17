#!/bin/bash
#使用sparkbatch程序读取es文件，将数据存到对应的表中
spark-submit \
  --master yarn  \
  --conf spark.app.name=GoodsCatAttributeNew \
  --conf spark.dynamicAllocation.maxExecutors=10 \
  --class com.vova.bigdata.sparkbatch.dataprocess.pdb.GoodsCatAttributeNew s3://vomkt-emr-rec/muren/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar  \
  --envFile prod

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi