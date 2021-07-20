#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

# https://confluence.gitvv.com/pages/viewpage.action?pageId=21268801
# spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.maxExecutors=150 --conf spark.yarn.maxAppAttempts=1 --name mlb_goods_score --class com.vova.rec.model.goods_score.score s3://vova-mlb/REC/util/mlb_goods_score.jar

# 原 python 代码重构， 消息修改
###依赖的表：mlb.mlb_vova_rec_goods_scorebase_data_d
spark-submit \
--master yarn --deploy-mode cluster \
--conf spark.dynamicAllocation.maxExecutors=150 \
--conf spark.yarn.maxAppAttempts=1 \
--name vova_mlb_goods_score_gongrui_chenkai \
--class com.vova.rec.model.goods_score.score \
s3://vova-mlb/REC/util/mlb_goods_score.jar

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`