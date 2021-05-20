#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为当天
if [ ! -n "$1" ]; then
  cur_date=$(date -d "-0 day" +%Y/%m/%d/00)
fi
spark-submit --master yarn \
--deploy-mode cluster \
--executor-memory 8G \
--packages com.snowplowanalytics:snowplow-scala-analytics-sdk_2.11:0.4.1 \
--name als_gr \
--class com.vova.rec.model.als.Main  s3://vova-mlb/REC/util/rec-training.jar \
${cur_date}
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi