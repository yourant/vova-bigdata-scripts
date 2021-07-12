#bin/sh
table="ads_fd_goods_picture_test"
user="longgu"

base_path="/mnt/vova-bigdata-scripts/fd/ads"

shell_path="${base_path}/${table}"

hive -f ${shell_path}/${table}_create.hql
spark-sql \
  --conf "spark.app.name=${table}" \
  --conf "spark.dynamicAllocation.maxExecutors=150" \
  -f ${shell_path}/${table}_insert.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "table [$user] [table] is finished !"