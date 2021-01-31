#!/bin/sh
flume_path="s3://bigdata-offline/warehouse/pdb"
base_path="/mnt/vova-bigdata-scripts/fd/ods/snowplow"

home=$(dirname "$0")
cd $home

hour_delta=0
hour_range=1

ts=$(date -d "$1" +"%Y-%m-%d %H")
pt_now=$ts

if [ "$#" -ne 3 ]; then
  pt=$(date -d "$ts -1 hours" +"%Y-%m-%d")
  hour=$(date -d "$ts -1 hours" +"%H")
  start=$(date -d "$ts -1 hours" +"%Y-%m-%d %H:00:00")
  end=$(date -d "$ts" +"%Y-%m-%d %H:00:00")
else
  pt=$2
  hour=$3
  start=$(date -d "${pt} ${hour} -1 hours" +"%Y-%m-%d %H:00:00")
  end=$(date -d "${pt} ${hour}" +"%Y-%m-%d %H:00:00")
fi

#filter
pt_filter=""
for ((i = hour_range + hour_delta; i > 0; i--)); do
  pt_filter=$pt_filter" pt = \"$(date -d "$pt_now - $i hours" +"%Y-%m-%d")\" and hour = \"$(date -d "$pt_now - $i hours" +"%H")\" or"
done
#去掉多余 or
pt_filter="("${pt_filter:0:-2}")"

#hive sql中使用的变量
echo "now:    " $pt_now
echo "start:  " $start
echo "end:    " $end
echo "pt:     (pt: ${pt},hour: ${hour})"
echo "filter: " $pt_filter
