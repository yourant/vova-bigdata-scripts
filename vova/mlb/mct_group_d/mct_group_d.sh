#!/bin/bash

pt=${1}
isUpdateDatabase=${2}
# 若无日期传入，默认取前一天
if [ ! -n "$1" ]; then
  pt=$(date -d "-1 day" +%Y-%m-%d)
  isUpdateDatabase=true
fi

# 前置依赖表：dim层，dwd.dwd_vova_fact_mbrmct_mct_cd
spark-submit --name mlb_vova_algo_mct_group_d_shudeyou --deploy-mode client --master yarn --driver-cores 1 --driver-memory 2G --class com.vova.mct_group.Main s3://vova-mlb/REC/util/mct_group.jar ${pt} ${isUpdateDatabase}


if [ $? -ne 0 ]; then
  exit 1
fi
