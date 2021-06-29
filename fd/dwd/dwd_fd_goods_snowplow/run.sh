#!/bin/sh
base_path="/mnt/vova-bigdata-scripts/fd/dwd"
table="dwd_fd_goods_snowplow_detail"
user="longgu"
ts=$(date +"%Y-%m-%d %H")


if [ "$#" -ne 3 ]; then
  pt=$(date -d "$ts -1 hours" +"%Y-%m-%d")
  hour=$(date -d "$ts -1 hours" +"%H")
else
  pt=$1
  hour=$2
fi

#hive sql中使用的变量
echo "pt:     (pt: ${pt},hour: ${hour})"

shell_path="${base_path}/${table}"

hive -f ${shell_path}/${table}_create.hql
spark-sql \
  --conf "spark.app.name=${table}_${user}" \
  --conf "spark.dynamicAllocation.maxExecutors=150" \
  -d pt="${pt}" \
  -d hour="${hour}" \
  -f ${shell_path}/${table}_insert.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "table [$table] is finished !"


