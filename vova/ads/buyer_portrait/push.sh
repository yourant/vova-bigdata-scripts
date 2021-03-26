#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y/%m/%d/00`
echo "pt=${cur_date}"
fi
pt=`date -d "-1 day" +%Y-%m-%d`
echo "$pt"
spark-submit \
--master yarn --deploy-mode client \
--executor-memory 8G \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf spark.app.name=ads_vova_user_push_d_zhangyin \
--class com.vova.bigdata.sparkbatch.dataprocess.ads.UserPushPortrait s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar \
--env product --pt $cur_date --no_pt 2020-11-03

if [ $? -ne 0 ];then
  echo "goods_pic_similar job error"
  exit 1
fi
