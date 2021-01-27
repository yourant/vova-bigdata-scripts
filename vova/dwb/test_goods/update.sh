#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pre_date=`date -d "-1 day" +%Y/%m/%d/%H`
fi
echo "$pre_date"


spark-submit --master yarn \
--conf spark.dynamicAllocation.maxExecutors=100 \
--conf spark.app.name=rpt_test_goods_result \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.bigdata.sparkbatch.dataprocess.dwb.TestGoodsRptMainOld s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar \
${pre_date}

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

