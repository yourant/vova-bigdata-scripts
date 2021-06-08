#!/bin/bash
#指定日期和引擎
path=$1
tableName=$2
catId=$3

spark-submit \
  --master yarn  \
  --conf spark.app.name=GoodsCatAttribute_$catId \
  --conf spark.dynamicAllocation.maxExecutors=10 \
  --class com.vova.bigdata.sparkbatch.dataprocess.pdb.GoodsCatAttribute s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar  \
  --envFile prod \
  --path $path \
  --tableName $tableName \
  --catId $catId

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi