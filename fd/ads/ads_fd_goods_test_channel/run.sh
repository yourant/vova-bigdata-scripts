#bin/sh
table="ads_fd_goods_test_channel"
user="longgu"

base_path="/mnt/vova-bigdata-scripts/fd/ads"

if [ "$#" -ne 2 ]; then
  today=$(date +"%Y-%m-%d")
  start_day=$(date -d "$today -1 weeks" +"%Y-%m-%d")
  which_day=$(date -d $start_day +"%w")
  pt_begin=$(date -d "$start_day -$[${which_day} - 1] days" +"%Y-%m-%d")
  pt_end=$(date -d "$pt_begin 144 hours" +"%Y-%m-%d")
else
  pt_begin=$1
  pt_end=$2
fi

echo "pt_begin: ${pt_begin}"
echo "pt_end: ${pt_end}"

shell_path="${base_path}/${table}"

hive -f ${shell_path}/${table}_create.hql
spark-sql \
  --conf "spark.app.name=${table}_${user}" \
  --conf "spark.dynamicAllocation.maxExecutors=100" \
  -d pt_begin="${pt_begin}" \
  -d pt_end="${pt_end}" \
  -f ${shell_path}/${table}_insert.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "table [$table] is finished !"


