#bin/sh
table="dim_fd_goods_finder"
user="lujiaheng"

base_path="/mnt/vova-bigdata-scripts/fd/dim"

if [ ! -n "$1" ]; then
  pt=$(date -d "- 1 days" +"%Y-%m-%d")
else
  echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d "$1" +"%Y-%m-%d" >/dev/null
  if [[ $? -ne 0 ]]; then
    echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
    exit
  fi
  pt=$1
fi
echo "pt: ${pt}"

shell_path="${base_path}/${table}"

hive -f ${shell_path}/${table}_create.hql

spark-sql \
  --conf "spark.app.name=${table}_${user}" \
  --conf "spark.dynamicAllocation.maxExecutors=60" \
  -f ${shell_path}/${table}_insert.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "table [$table] is finished !"
