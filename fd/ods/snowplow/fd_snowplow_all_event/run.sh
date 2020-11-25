#!/bin/sh
path="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $path/../snowplow_run_common.sh

table=ods_fd_snowplow_all_event
user=lujiaheng

shell_path="$base_path/fd_snowplow_all_event"

#将flume收集的数据存到tmp表中
hive -hiveconf flume_path=$flume_path -f ${shell_path}/pdb_fd_snowplow_offline.hql
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