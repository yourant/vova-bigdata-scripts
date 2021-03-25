#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
echo "pt=${pt}"
fi

spark-submit \
--master yarn \
--num-executors 10 \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.app.name=ads_vova_goods_binlog_monitor_zhangyin \
--class com.vova.bigdata.sparkbatch.dataprocess.ads.GoodsBinlogMonitor s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar \
--env product --op parseLog  --basePath s3://bigdata-offline/warehouse/pdb/vova/vovadbthemis/themis/Vovavovadbthemischange-themis-goods/  --pt $pt

if [ $? -ne 0 ];then
  echo "goods_changed_record_d job error"
  exit 1
fi