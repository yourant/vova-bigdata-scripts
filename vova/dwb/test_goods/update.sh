#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pre_date=`date -d "-1 day" +%Y/%m/%d/%H`
fi
echo "$pre_date"


spark-submit --master yarn --deploy-mode cluster \
--conf spark.dynamicAllocation.maxExecutors=100 \
--conf spark.app.name=dwb_vova_test_goods_result_zhangyin \
--class com.vova.bigdata.sparkbatch.dataprocess.dwb.TestGoodsRptMain s3://vomkt-emr-rec/jar/test-goods-rpt-1.0.0.jar \
${pre_date}

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

spark-submit --master yarn \
--conf spark.dynamicAllocation.maxExecutors=100 \
--conf spark.app.name=dwb_vova_test_goods_month_performance_zhangyin \
--class com.vova.bigdata.sparkbatch.dataprocess.dwb.TestGoodsMonthPerformanceMain \
s3://vomkt-emr-rec/jar/test-goods-rpt-m-1.0.0.jar \
${pre_date}

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

