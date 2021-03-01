#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y/%m/%d/%H`
echo "pt=${pt}"
fi

spark-submit \
--master yarn --deploy-mode cluster \
--driver-memory 9G \
--num-executors 10 \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.app.name=ads_vova_buyer_goods_rating_to_redis \
--class com.vova.bigdata.sparkbatch.dataprocess.ads.UserGoodsRating s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar \
--env product --pt $pt

if [ $? -ne 0 ];then
  echo "user_goods_rating_to_redis job error"
  exit 1
fi
