#bin/sh
table="dwb_fd_erp_dispatch_link_report"
user="ruimeng"

base_path="/mnt/vova-bigdata-scripts/fd/dwb"

if [ ! -n "$1" ]; then
  pt=$(date -d "- 1 days" +"%Y-%m-%d")
  Hour=$(date +"%H")
else
  echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d "$1" +"%Y-%m-%d" >/dev/null
  if [[ $? -ne 0 ]]; then
    echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
    exit
  fi
  pt=$1
  Hour=$2
fi
echo "pt: ${pt}"


shell_path="${base_path}/${table}"

# hive -f ${shell_path}/${table}_create.hql
# $2为执行的小时，在09 和21 点执行,判断执行时间是否小于13点，utc时间是否小于13点是的执行09的批次，不是执行21点的批次


if [ ${Hour} -lt '13' ]; then

  hour_str='09:00:00'
  echo "Hour: ${hour_str}"
  spark-sql \
  --conf "spark.app.name=${table}_${user}" \
  --conf "spark.dynamicAllocation.maxExecutors=60" \
  -d pt="${pt}" \
  -d hour_str="${hour_str}" \
  -f ${shell_path}/${table}_insert01.sql
else
   hour_str='21:00:00'
   echo "Hour: ${hour_str}"
   spark-sql \
  --conf "spark.app.name=${table}_${user}" \
  --conf "spark.dynamicAllocation.maxExecutors=60" \
  -d pt="${pt}" \
  -d hour_str="${hour_str}" \
  -f ${shell_path}/${table}_insert02.sql
fi



if [ $? -ne 0 ]; then
  exit 1
fi
echo "table [$table] is finished !"
