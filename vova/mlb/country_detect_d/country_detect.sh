#!/bin/bash
pt=$1

if [ ! -n "$1" ];then
   pt=`date -d "-1 day" +%Y/%m/%d`
fi

spark-submit  --driver-cores 3 --driver-memory 10G --executor-cores 3 --executor-memory 10G --conf spark.driver.memoryOverhead=4096 --conf spark.executor.memoryOverhead=4096 --conf spark.sql.session.timeZone=UTC --conf spark.yarn.maxAppAttempts=1 --class com.vova.Main s3://vova-mlb/REC/util/country-recognition.jar ${pt}

if [ $? -ne 0 ]; then
  exit 1
fi

spark-submit  --driver-cores 3 --driver-memory 10G --executor-cores 3 --executor-memory 10G --conf spark.driver.memoryOverhead=4096 --conf spark.executor.memoryOverhead=4096 --conf spark.sql.session.timeZone=UTC --conf spark.yarn.maxAppAttempts=1 --class com.vova.model.CleanPredictCountryRedis s3://vova-mlb/REC/util/country-recognition.jar

if [ $? -ne 0 ]; then
  exit 1
fi
