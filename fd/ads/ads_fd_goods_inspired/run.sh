#bin/sh
table="ads_fd_goods_inspired"
user="lujiaheng"

base_path="/mnt/vova-bigdata-scripts/fd/ads"

if [ ! -n "$1" ]; then
  pt=$(date -d "- 1 hours" +"%Y-%m-%d")
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
pt_yesterday=$(date -d "-1 day" +"%Y-%m-%d")
batchNum_new=`hive -e " select max(batch) from ads.ads_fd_goods_inspired where pt >='${pt_yesterday}' "`
batchNum=`expr substr $batchNum_new 1 8`
spark-sql \
  --conf "spark.app.name=${table}_${user}" \
  --conf "spark.dynamicAllocation.maxExecutors=60" \
  -d pt="${pt}" \
  -d batchNum="${batchNum}" \
  -f ${shell_path}/${table}_insert.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "table [$table] is finished !"
