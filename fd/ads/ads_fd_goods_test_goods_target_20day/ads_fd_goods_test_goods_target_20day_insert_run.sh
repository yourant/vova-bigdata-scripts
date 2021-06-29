#bin/sh
table="ads_fd_goods_test_goods_target_20day"
user="longgu"

base_path="/mnt/vova-bigdata-scripts/fd/ads"

shell_path="${base_path}/${table}"

spark-sql \
  --conf "spark.app.name=ads_fd_goods_test_goods_target_20day_insert_${user}" \
  --conf "spark.dynamicAllocation.maxExecutors=150" \
  -f ${shell_path}/ads_fd_goods_test_goods_target_20day_insert.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "table [$table] is finished !"


