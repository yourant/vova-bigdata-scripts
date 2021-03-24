#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  cur_date=$(date -d "-0 day" +%Y/%m/%d/00)
fi
###依赖的表：dwd.fact_pay，dwd.dim_goods，dws.dws_buyer_portrait，ods.vova_activity_coupon_tag
spark-submit --master yarn \
--name ads_algo_usercall_d \
--conf spark.sql.autoBroadcastJoinThreshold=10485760 \
--deploy-mode client \
--conf spark.dynamicAllocation.maxExecutors=150 \
--class com.vova.rec.data.Main s3://vova-mlb/REC/util/rec-training.jar  ${cur_date}
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
