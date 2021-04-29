#!/bin/bash
echo "start_time:" $(date +"%Y-%m-%d %H:%M:%S" -d "8 hour")

# mlb.mlb_vova_rec_m_user_kg_tag_d
# 取数jar包位置： s3://vova-mlb/REC/util/user_kg_tag.jar
###依赖的表： dws.dws_vova_buyer_goods_behave
spark-submit \
--master yarn \
--deploy-mode cluster \
--executor-cores 1 \
--executor-memory 6G \
--conf spark.dynamicAllocation.maxExecutors=100 \
--name mlb_vova_rec_m_user_kg_tag_d_gongrui_chenkai \
--class com.vova.model.knowgraph \
s3://vova-mlb/REC/util/user_kg_tag.jar

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
echo "end_time:" $(date +"%Y-%m-%d %H:%M:%S" -d "8 hour")
