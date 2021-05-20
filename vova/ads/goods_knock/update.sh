#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
fi
echo "pt=$pt"

spark-submit --master yarn \
--class com.vova.bigdata.sparkbatch.dataprocess.ads.BrandKnock \
--conf "spark.dynamicAllocation.maxExecutors=50" \
--conf "spark.app.name=vova_brand_knock_zhangyin" \
s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar  \
--env product --pt $pt --knocks 'andy.zhang,Buyue,Qiezi'

if [ $? -ne 0 ];then
  exit 1
fi