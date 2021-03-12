#bin/sh
table="ads_fd_rec_goods_age_group"
user="lujiaheng"

base_path="/mnt/vova-bigdata-scripts/fd/ads"

ts=$(date -d "$1" +"%Y-%m-%d %H")

if [ "$#" -ne 3 ]; then
  pt=$(date -d "$ts" +"%Y-%m-%d")
else
  pt=$2
fi

echo "pt: ${pt}"

shell_path="${base_path}/${table}"

hive -f ${shell_path}/create.hql
spark-sql \
  --conf "spark.app.name=${table}_${user}" \
  --conf "spark.dynamicAllocation.maxExecutors=60" \
  -d pt="${pt}" \
  -f ${shell_path}/insert.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "table [$table] is finished !"


