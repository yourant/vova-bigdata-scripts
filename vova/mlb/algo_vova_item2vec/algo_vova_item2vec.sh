#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为当天
if [ ! -n "$1" ]; then
  cur_date=$(date -d "-0 day" +%Y/%m/%d/00)
fi
###依赖的表 dwd.fact_log_goods_click,dwd.fact_log_common_click,dwd.fact_pay,ods.vova_virtual_goods,dwd.fact_log_page_view_arc,dwd.dim_goods,ads.ads_goods_id_behave_2m
spark-submit --name Item2Vec_TRAINING --deploy-mode client --master yarn --driver-cores 3 --driver-memory 20G --conf spark.driver.maxResultSize=10g --conf spark.kryoserializer.buffer.max=512m --conf spark.sql.autoBroadcastJoinThreshold=10485760 --packages com.snowplowanalytics:snowplow-scala-analytics-sdk_2.11:0.4.1 --conf spark.dynamicAllocation.maxExecutors=150 --class com.vova.rec.model.item2vec.SessionTrainer s3://vomkt-emr-rec/DB_data/recall/jar/rec-training.jar ${cur_date}
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
