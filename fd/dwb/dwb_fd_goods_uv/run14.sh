#bin/sh
table="dwb_fd_goods_snowplow_uv"
tmp_table="tmp_fd_goods_uv"
user="longgu"

base_path="/mnt/vova-bigdata-scripts/fd/dwb"

ts=$(date +"%Y-%m-%d %H")
if [ "$#" -ne 4 ]; then
  pt_end=$(date -d "$ts -16 hours" +"%Y-%m-%d")
  pt_begin=$(date -d "$ts -352 hours" +"%Y-%m-%d")
  pt_one=$(date -d "$ts -328 hours" +"%Y-%m-%d")
  pt_two=$(date -d "$ts -40 hours" +"%Y-%m-%d")
else
  pt_end=$1
  pt_begin=$2
  pt_one=$3
  pt_two=$4
fi

echo "pt_end: ${pt_end}"
echo "pt_begin: ${pt_begin}"
echo "pt_one: ${pt_one}"
echo "pt_two: ${pt_two}"

shell_path="${base_path}/${table}"

hive -f ${shell_path}/${tmp_table}_create.hql
spark-sql \
  --conf "spark.app.name=${tmp_table}_14" \
  --conf "spark.dynamicAllocation.maxExecutors=150" \
  -d pt_end="${pt_end}" \
  -d pt_begin="${pt_begin}" \
  -d pt_one="${pt_one}" \
  -d pt_two="${pt_two}" \
  -f ${shell_path}/${tmp_table}_insert.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "table [$user] [tmp_table] is finished !"
