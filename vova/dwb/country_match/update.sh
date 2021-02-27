#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
fi

spark-submit \
--master yarn \
--deploy-mode client \
--conf spark.dynamicAllocation.maxExecutors=150 \
--name countryFromRedis \
--class com.vova.bigdata.sparkbatch.dataprocess.dwb.AnalyticCountryFromRedis \
s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar ${pt}
if [ $? -ne 0 ];then
  exit 1
fi
