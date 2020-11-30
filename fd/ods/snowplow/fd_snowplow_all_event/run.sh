#!/bin/sh
path="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source $path/../snowplow_run_common.sh

table=ods_fd_snowplow_all_event
user=lujiaheng

shell_path="$base_path/fd_snowplow_all_event"

saprk_pt=$(date -d "$pt_now - 1 hours" +"%Y/%m/%d/%H")
pdb_pt=$(date -d "$pt_now - 1 hours" +"%Y-%m-%d")
pdb_hour=$(date -d "$pt_now - 1 hours" +"%H")

echo "saprk_pt: $saprk_pt"
echo "partition: (pt='${pdb_pt}',hour='${pdb_hour}')"

#hive -e "alter table pdb.pdb_fd_snowplow_offline drop if exist partition(pt='${pdb_pt}',hour='${pdb_hour}')"

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf "spark.dynamicAllocation.maxExecutors=120" \
  --conf spark.executor.memory=4096M \
  --conf spark.yarn.appMasterEnv.sparkMaster=yarn \
  --conf spark.yarn.appMasterEnv.appName=FDSnowplowOffline \
  --conf spark.yarn.appMasterEnv.rawDataPath=s3://artemis-evt/enrich-good \
  --conf spark.yarn.appMasterEnv.savePath=s3://bigdata-offline/warehouse/pdb/fd/snowplow/snowplow_offline \
  --conf spark.yarn.appMasterEnv.start=$saprk_pt \
  --conf spark.app.name=FDSnowplowOffline \
  --class com.fd.bigdata.sparkbatch.log.jobs.SnowplowOffline \
  s3://vomkt-emr-rec/jar/warehouse/fd/snowplow_offline_1.2.jar

echo "spark finished"

hive -e "MSCK REPAIR TABLE pdb.pdb_fd_snowplow_offline;"
#hive -e "alter table pdb.pdb_fd_snowplow_offline add partition(pt='${pdb_pt}',hour='${pdb_hour}')"

if [ $? -ne 0 ]; then
  exit 1
fi
echo "step1: pdb_fd_snowplow_offline table is finished !"

#将打点数据放到对应的小时里面
hive -f ${shell_path}/${table}_create.hql

spark-sql \
  --conf "spark.app.name=${table}_${user}" \
  --conf "spark.dynamicAllocation.maxExecutors=60" \
  -d start="$start" \
  -d end="$end" \
  -d pt_filter="$pt_filter" \
  -f ${shell_path}/${table}_insert.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "step2: $table table is finished !"
