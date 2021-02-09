#!/bin/bash
#指定日期和引擎
event_name="traff_control"
day=$1
#默认日期为昨天
if [ ! -n "$1" ];then
day=`date "+%Y-%m-%d %H:%M:%S"`
fi
echo "day=$day"
time=`date -d "$day" +%Y-%m-%d-%H:%M`
echo "time=$time"
minte=`date -d "$day" +%M`
echo "minte=$minte"
hour=`date -d "$day" +%H`
echo "hour=$hour"
if [[ "$hour" == "03" && "$minte" > "30" ]]; then
    spark-submit --master yarn --deploy-mode cluster --queue important  --driver-memory 2G --num-executors 10 --conf spark.dynamicAllocation.enabled=false --conf spark.driver.memoryOverhead=2048 --conf spark.executor.memoryOverhead=2048 --conf spark.default.parallelism=300 --conf spark.sql.shuffle.partitions=300 --conf spark.sql.session.timeZone=UTC --packages com.snowplowanalytics:snowplow-scala-analytics-sdk_2.11:0.4.1 --name traffic_control_h  --class com.vova.model.UpdateProb s3://vomkt-emr-rec/jar/traffic_control.jar $time init
else
    spark-submit --master yarn --deploy-mode cluster --queue important --driver-memory 2G  --num-executors 10 --conf spark.dynamicAllocation.enabled=false --conf spark.driver.memoryOverhead=2048 --conf spark.executor.memoryOverhead=2048 --conf spark.default.parallelism=300 --conf spark.sql.shuffle.partitions=300 --conf spark.sql.session.timeZone=UTC --packages com.snowplowanalytics:snowplow-scala-analytics-sdk_2.11:0.4.1 --name traffic_control_h  --class com.vova.model.UpdateProb s3://vomkt-emr-rec/jar/traffic_control.jar $time update
fi
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  echo "${event_name} job error"
  exit 1
fi