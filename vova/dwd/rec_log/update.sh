#!/bin/bash
#指定日期和引擎
name="query_from_es"
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
fi

spark-submit --master yarn  --deploy-mode client --driver-memory 10G --conf spark.dynamicAllocation.maxExecutors=50 --name vova-dwd-queryFromES  --class com.vova.bigdata.sparkbatch.dataprocess.dwd.QueryFromEs s3://vomkt-emr-rec/jar/vova-bigdata-dwd-queryFromES.jar ${pt}
if [ $? -ne 0 ];then
  exit 1
fi