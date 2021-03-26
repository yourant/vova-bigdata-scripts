#!/bin/bash
#指定日期和引擎
#商家团伙
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  cur_date=$(date -d "-1 day" +%Y-%m-%d/)
fi
###依赖的表：dwd.dim_merchant，dwd.fact_mbrmct_mct_cd
spark-submit \
--name ads_algo_mct_group_d \
--deploy-mode client \
--master yarn \
--driver-cores 1 \
--driver-memory 2G \
--conf spark.dynamicAllocation.maxExecutors=150 \
--class com.vova.model.Main s3://vova-mlb/REC/util/rec-training.jar $cur_date

#如果脚本失败，则报错

if [ $? -ne 0 ]; then
  exit 1
fi
