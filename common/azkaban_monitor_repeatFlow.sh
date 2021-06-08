#!/bin/bash
spark-submit  \
--conf spark.executor.memory=2g \
--conf spark.dynamicAllocation.maxExecutors=10 \
--conf spark.app.name=MonitorAzkaban \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.monitor.MonitorAzkabanRepeatFlow  s3://vomkt-emr-rec/jar/monitor_azkaban.jar

if [ $? -ne 0 ]; then
  exit 1
fi