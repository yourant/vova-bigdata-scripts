#!/bin/sh
if [ ! -n "$1" ]; then
  pt_now=$(date +"%Y-%m-%d")
  pt=$(date -d "- 1 days" +"%Y-%m-%d")
else
  echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d "$1" +"%Y-%m-%d" >/dev/null
  if [[ $? -ne 0 ]]; then
    echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
    exit
  fi
   pt_now=$1
   pt=$1
fi


echo "now:  " $pt_now
echo "pt:   " $pt

shell_path="/mnt/vova-bigdata-scripts/fd/dwd/dwd_fd_session_channel"

hive -f ${shell_path}/dwd_fd_session_channel.hql
if [ $? -ne 0 ]; then
  exit 1
fi
echo "step1: create table finished !"

spark-submit \
--master yarn \
--deploy-mode cluster \
--conf spark.yarn.appMasterEnv.sparkMaster=yarn \
--conf spark.yarn.appMasterEnv.appName=FDSessionGAChannel \
--conf spark.yarn.appMasterEnv.sourceTable=ods_fd_snowplow.ods_fd_snowplow_view_event \
--conf spark.yarn.appMasterEnv.targetTable=dwd.dwd_fd_session_channel \
--conf spark.yarn.appMasterEnv.pt=$pt \
--conf spark.sql.shuffle.partitions=380 \
--conf spark.app.name=FDSessionGAChannel \
--class com.fd.bigdata.sparkbatch.log.jobs.SessionGAChannel \
s3://vomkt-emr-rec/jar/warehouse/fd/snowplow_offline_1.6.jar

echo "step2: insert data finished !"