#bin/sh
table="ads_fd_goods_target_14day"
user="longgu"

base_path="/mnt/vova-bigdata-scripts/fd/ads"

ts=$(date +"%Y-%m-%d %H")
if [ "$#" -ne 4 ]; then
  pt_end=$(date +"%Y-%m-%d")
  pt_begin=$(date -d "$ts -336 hours" +"%Y-%m-%d")
  time_end=$(date +"%Y-%m-%d 07:00:00")
  time_begin=$(date -d "$ts -336 hours" +"%Y-%m-%d 07:00:00")
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

spark-sql \
  --conf "spark.app.name=${table}_tmp" \
  --conf "spark.dynamicAllocation.maxExecutors=150" \
  -d pt_begin="${pt_begin}" \
  -d pt_end="${pt_end}" \
  -d time_begin="${time_begin}" \
  -d time_end="${time_end}" \
  -f ${shell_path}/${table}_tmp_insert.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "table [$table] is finished !"


!(impressions=0 and clicks=0 and users=0 and sales_order=0 and detail_add_cart=0 and list_add_cart=0 and checkout=0 and sales=0)