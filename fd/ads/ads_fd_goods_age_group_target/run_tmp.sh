#bin/sh
table="ads_fd_goods_age_group_target"
user="longgu"

base_path="/mnt/vova-bigdata-scripts/fd/ads"

ts=$(date +"%Y-%m-%d %H")
if [ "$#" -ne 2 ]; then
  pt_begin=$(date -d "$ts -720 hours" +"%Y-%m-%d")
  pt_end=$(date -d "$ts -24 hours" +"%Y-%m-%d")
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
  --conf "spark.dynamicAllocation.maxExecutors=150" \
  -d pt_begin="${pt_begin}" \
  -d pt_end="${pt_end}" \
  -f ${shell_path}/${table}_temp_insert.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "table [$table] is finished !"


