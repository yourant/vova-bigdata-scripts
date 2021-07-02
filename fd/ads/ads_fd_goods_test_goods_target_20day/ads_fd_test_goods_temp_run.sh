#bin/sh
table="ads_fd_goods_test_goods_target_20day"
user="longgu"

base_path="/mnt/vova-bigdata-scripts/fd/ads"

ts=$(date +"%Y-%m-%d %H")
if [ "$#" -ne 4 ]; then
  pt_begin=$(date +"%Y-%m-%d")
  pt_end=$(date -d "$ts -480 hours" +"%Y-%m-%d")
  time_begin=$(date +"%Y-%m-%d %H:00:00")
  time_end=$(date -d "$ts -480 hours" +"%Y-%m-%d %H:00:00")
else
  pt_begin=$1
  pt_end=$2
  time_begin=$3
  time_end=$4
fi

echo "pt_begin: ${pt_begin}"
echo "pt_end: ${pt_end}"
echo "time_begin: ${time_begin}"
echo "time_end: ${time_end}"

shell_path="${base_path}/${table}"

hive -f ${shell_path}/${table}_create.hql
spark-sql \
  --conf "spark.app.name=ads_fd_test_goods_temp_${user}" \
  --conf "spark.dynamicAllocation.maxExecutors=150" \
  -d pt_begin="${pt_begin}" \
  -d pt_end="${pt_end}" \
  -d time_begin="${time_begin}" \
  -d time_end="${time_end}" \
  -f ${shell_path}/ads_fd_test_goods_temp.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "table [$table] is finished !"


