#!/bin/sh
home=`dirname "$0"`
cd $home

ts=$(date -d "$1" +"%Y-%m-%d %H")

if [ "$#" -ne 3 ]; then
  pt=$(date -d "$ts -1 hours" +"%Y-%m-%d")
  hour=$(date -d "$ts -1 hours" +"%H")
else
  pt=$2
  hour=$3
fi

#hive sql中使用的变量
echo $pt

#脚本路径
shell_path="/mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_realtime_new_rpt"

spark-sql \
--conf "spark.app.name=dwb_fd_realtime_gmv_orders_rpt_zhangchenhao"   \
-d pt=$pt \
-f ${shell_path}/order_info_inc.hql

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo " order_inc   table is finished !"