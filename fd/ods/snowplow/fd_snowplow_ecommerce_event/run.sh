#!/bin/sh
path="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source $path/../snowplow_run_common.sh

table=ods_fd_snowplow_ecommerce_event
user=lujiaheng

shell_path="$base_path/fd_snowplow_ecommerce_event"

hive -f ${shell_path}/${table}_create.hql

spark-sql \
  --conf "spark.app.name=${table}_${user}" \
  --conf "spark.dynamicAllocation.maxExecutors=100" \
  --conf spark.executor.memoryOverhead=2048 \
  --conf spark.executor.memory=6g \
  -d pt_filter="$pt_filter" \
  -d pt="${pt}" \
  -d hour="${hour}" \
  -f ${shell_path}/${table}_insert.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "$table table is finished !"
