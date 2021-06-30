#bin/sh
table="ads_fd_goods_display_order_artemis_language"
user="longgu"

base_path="/mnt/vova-bigdata-scripts/fd/ads"

ts=$(date +"%Y-%m-%d %H")
if [ "$#" -ne 2 ]; then
  pt_end=$(date -d "$ts -16 hours" +"%Y-%m-%d")
  pt_begin=$(date -d "$ts -352 hours" +"%Y-%m-%d")
else
  pt_end=$1
  pt_begin=$2
fi

echo "pt_end: ${pt_end}"
echo "pt_begin: ${pt_begin}"

shell_path="${base_path}/${table}"

hive -f ${shell_path}/${table}_create.hql
spark-sql \
  --conf "spark.app.name=${table}" \
  --conf "spark.dynamicAllocation.maxExecutors=150" \
  -d pt="14" \
  -d pt_end="${pt_end}" \
  -d pt_begin="${pt_begin}" \
  -f ${shell_path}/${table}_insert.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "table [$user] [table] is finished !"