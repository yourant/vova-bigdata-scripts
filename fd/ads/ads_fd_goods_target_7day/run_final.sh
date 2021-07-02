#bin/sh
table="ads_fd_goods_target_7day"
user="longgu"

base_path="/mnt/vova-bigdata-scripts/fd/ads"
shell_path="${base_path}/${table}"

spark-sql \
  --conf "spark.app.name=${table}_final" \
  --conf "spark.dynamicAllocation.maxExecutors=150" \
  -f ${shell_path}/${table}_final_insert.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "table [$table] is finished !"


