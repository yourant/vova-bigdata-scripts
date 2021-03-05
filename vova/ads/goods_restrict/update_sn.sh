#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
fi
spark-submit --master yarn  \
--conf "spark.app.name=ads_vova_restrict_sntogsn_zhangyin" \
--conf "spark.dynamicAllocation.maxExecutors=10" \
--class com.vova.bigdata.sparkbatch.dataprocess.ads.GoodsRestrictSnToGsn \
s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar \
--env product --pt $pt --url https://merchant.vova.com.hk/api/v1/internal/Product/snToGsn --secret_key wik9Ooghe5AilaGhahjaPoothahk

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi