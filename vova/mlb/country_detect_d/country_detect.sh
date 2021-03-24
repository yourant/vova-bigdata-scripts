#!/bin/bash
pt=$1

if [ ! -n "$1" ];then
   pt=`date -d "-1 day" +%Y/%m/%d`
fi

spark-submit  --driver-cores 1 --driver-memory 4G  --executor-memory 8G --name mlb_vova_country_detect_d_shudeyou   --conf spark.dynamicAllocation.maxExecutors=100  --conf spark.sql.session.timeZone=UTC --conf spark.yarn.maxAppAttempts=1 --class com.vova.Main s3://vova-mlb/REC/util/country-recognition.jar ${pt}

if [ $? -ne 0 ]; then
  exit 1
fi

spark-submit  --driver-cores 1 --driver-memory 4G  --executor-memory 8G  --name mlb_vova_country_detect_redis_d_shudeyou  --conf spark.dynamicAllocation.maxExecutors=100 --conf spark.sql.session.timeZone=UTC --conf spark.yarn.maxAppAttempts=1 --class com.vova.model.CleanPredictCountryRedis s3://vova-mlb/REC/util/country-recognition.jar

if [ $? -ne 0 ]; then
  exit 1
fi
