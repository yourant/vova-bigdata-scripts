#bin/sh
table="ads_fd_druid_goods"
user="lujiaheng"

base_path="/mnt/vova-bigdata-scripts/fd/ads"

ts=$(date -d "$1" +"%Y-%m-%d %H")

if [ "$#" -ne 3 ]; then
  pt=$(date -d "$ts -1 hours" +"%Y-%m-%d")
  hour=$(date -d "$ts -1 hours" +"%H")
else
  pt=$2
  hour=$3
fi

echo "pt: ${pt}"
echo "hour: ${hour}"

shell_path="${base_path}/${table}"

hive -f ${shell_path}/${table}_create.hql
spark-sql \
  --conf "spark.app.name=${table}_${user}" \
  --conf "spark.dynamicAllocation.maxExecutors=60" \
  -d pt="${pt}" \
  -d pt="${hour}" \
  -f ${shell_path}/${table}_insert.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "table [$table] is finished !"


