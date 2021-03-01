#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y/%m/%d/00`
echo "pt=${pt}"
fi
echo "spark-submit --master yarn \
 --conf spark.app.name=BackStageGoodsSn \
 --class com.vova.data.BackStageGoodsSn  \
 s3://vomkt-emr-rec/jar/BackStageGoodsSn.jar \
 --env product --pt $pt
"
spark-submit --master yarn \
 --conf "spark.dynamicAllocation.maxExecutors=100" \
 --conf spark.app.name=ads_vova_back_stage_goods_sn_d_zhangyin \
--class com.vova.bigdata.sparkbatch.dataprocess.ads.BackStageGoodsSn  \
s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar \
 --env product --pt $pt

if [ $? -ne 0 ];then
  echo "goods_pic_similar job error"
  exit 1
fi
