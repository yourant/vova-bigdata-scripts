#!/bin/bash
echo "start_time:" $(date +"%Y-%m-%d %H:%M:%S" -d "8 hour")

# mlb.mlb_vova_kg_d,mlb.mlb_vova_kg_no_user_d
# 取数jar包位置： s3://vova-mlb/REC/util/mlb_vova_kg_d.jar
###依赖的表：
# dws.dws_vova_buyer_goods_behave
#ads.ads_vova_buyer_portrait_feature
#ads.ads_vova_goods_attribute_merge
#ads.ads_vova_usable_value
#ads.ads_vova_bod
#ads.ads.ads_vova_scene_bod_original_explode_data
#ads.ads_vova_knowledge_graph_bod_goods_rank_data
#ads.ads_vova_scene_bod_goods_rank_data
#dim.dim_vova_buyers
spark-submit \
--master yarn \
--deploy-mode cluster \
--executor-cores 1 \
--conf spark.dynamicAllocation.maxExecutors=120 \
--name mlb_vova_kg_d_gongrui_murenqing \
--class com.vova.rec.model.knowledge_graph.kg \
s3://vova-mlb/REC/util/mlb_vova_kg_d.jar

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
echo "end_time:" $(date +"%Y-%m-%d %H:%M:%S" -d "8 hour")