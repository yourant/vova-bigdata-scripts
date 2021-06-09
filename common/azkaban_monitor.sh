#指定日期和引擎
cur_date=$1
#默认日期为今天
if [ ! -n "$1" ];then
   cur_date=`date +%Y-%m-%d`
fi

spark-submit  \
--conf spark.executor.memory=2g \
--conf spark.dynamicAllocation.maxExecutors=10 \
--conf spark.app.name=MonitorAzkaban \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.monitor.MonitorAzkaban  s3://vomkt-emr-rec/jar/monitor_azkaban_v5.jar \
--env local --date ${cur_date}


if [ $? -ne 0 ]; then
  exit 1
fi
