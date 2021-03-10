#!/bin/bash
#指定日期和引擎
stime=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  stime=`date -d "-1hour" "+%Y-%m-%d %H:00:00"`
fi
echo "$stime"
hour=`date -d "$stime" +%H`
echo "hour=$hour"
#默认小时
etime=$2
if [ ! -n "$1" ]; then
  etime=`date -d "0 hour" "+%Y-%m-%d %H:00:00"`
fi
echo "etime=$etime"
pt=`date -d "$etime" +%Y-%m-%d`
echo "pt=$pt"


spark-submit --master yarn \
--deploy-mode client \
--driver-memory 8G \
--executor-memory 8G \
--conf spark.dynamicAllocation.maxExecutors=100 \
--name "test_goods" \
--class com.vova.bigdata.sparkbatch.dataprocess.ads.GoodsTest s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar \
--stime "${stime}" --hour ${hour} --etime "${etime}" --pt ${pt} --env product
if [ $? -ne 0 ];then
  echo "test_goods job error"
  exit 1
fi
