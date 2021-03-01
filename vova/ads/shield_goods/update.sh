#!/bin/bash
#指定日期和引擎
name="shield_goods"
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
fi

spark-submit --master yarn \
--deploy-mode client  \
--driver-memory 9G \
--conf spark.dynamicAllocation.maxExecutors=50 \
--name ads_vova_shield_goods_zhangyin  \
--class com.vova.bigdata.sparkbatch.dataprocess.ads.ShieldGoodsFromInfluxDB \
s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar \
--env product --pt $pt --db vova_rec

if [ $? -ne 0 ];then
  exit 1
fi

