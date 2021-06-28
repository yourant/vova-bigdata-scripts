#!/bin/bash
echo "start_time:" $(date +"%Y-%m-%d %H:%M:%S" -d "8 hour")
cur_date=$1
cur_hour="00"
if [ ! -n "$1" ];then
cur_date=`date -d "-1 hour" +%Y-%m-%d`
cur_hour=`date -d "-1 hour" +%H`
fi

echo "cur_date: ${cur_date}"
echo "cur_hour: ${cur_hour}"

# mlb.mlb_vova_rec_u2i_match_list_h
###依赖的表： dws.dws_vova_buyer_goods_behave_h, dim.dim_vova_goods, mlb.mlb_vova_rec_i2i_match_d_d
spark-submit \
--master yarn \
--deploy-mode cluster \
--executor-cores 1 --executor-memory 6G \
--conf spark.dynamicAllocation.maxExecutors=150 \
--conf spark.yarn.maxAppAttempts=1 \
--name mlb_vova_list_u2i_h_gr_chenkai \
--class com.vova.rec.model.list.list_u2i \
s3://vova-mlb/REC/util/list_u2i.jar \
${cur_date} ${cur_hour} 100 30 20 0 3 5 6

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
echo "end_time:" $(date +"%Y-%m-%d %H:%M:%S" -d "8 hour")
