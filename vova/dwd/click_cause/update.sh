#!/bin/bash
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

spark-submit --master yarn   \
--conf spark.executor.memory=10g \
--conf spark.dynamicAllocation.maxExecutors=110 \
--conf spark.yarn.maxAppAttempts=1 \
--conf spark.sql.autoBroadcastJoinThreshold=-1 \
--conf spark.app.name=vova_dwd_goods_click_cause \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.bigdata.sparkbatch.dataprocess.dwd.GoodsClickCause s3://vomkt-emr-rec/jar/vova-bigdata-dwd-goodsClickCause.jar \
--envFile prod --pt ${cur_date}