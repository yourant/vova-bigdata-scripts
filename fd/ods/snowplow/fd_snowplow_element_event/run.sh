#!/bin/sh
home=$(dirname "$0")
cd $home

hour_delta=1
hour_range=6

if [ ! -n "$1" ]; then
  dt_now=$(date +"%Y-%m-%d %H")
else
  echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}" && date -d "$1" +"%Y-%m-%d %H:%M:%S" >/dev/null
  if [[ $? -ne 0 ]]; then
    echo "接收的时间格式${1}不符合:%Y-%m-%d %H:%M:%S，请输入正确的格式!"
    exit
  fi
  dt_now=$1
fi

#collector开始时间
start=$(date -d "$dt_now - $hour_range hours" +"%Y-%m-%d %H:00:00")
end=$(date -d "$dt_now" +"%Y-%m-%d %H:00:00")

#filter
dt_filter=""
for ((i = hour_range + hour_delta; i >= 0; i--)); do
  dt_filter=$dt_filter" dt = \"$(date -d "$dt_now - $i hours" +"%Y-%m-%d")\" and hour = \"$(date -d "$dt_now - $i hours" +"%H")\" or"
done
#去掉多余 or
dt_filter="("${dt_filter:0:-2}")"

#hive sql中使用的变量
echo "now:    " $dt_now
echo "start:  " $start
echo "end:    " $end
echo "filter: " $dt_filter

shell_path="/mnt/vova-bigdata-scripts/fd/ods/snowplow/fd_snowplow_element_event"

#筛选对应数据到对应表中
hive -hiveconf "dt_filter=$dt_filter" -f ${shell_path}/ods_fd_snowplow_element_event.hql
if [ $? -ne 0 ]; then
  exit 1
fi
echo "ods_fd_snowplow_element_event table is finished !"
