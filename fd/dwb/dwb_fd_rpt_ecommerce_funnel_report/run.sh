#!/bin/sh
if [ ! -n "$1" ]; then
  pt_now=$(date +"%Y-%m-%d")
  pt=$(date -d "$1 - 1 days " +"%Y-%m-%d")
else
  echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d "$1" +"%Y-%m-%d" >/dev/null
  if [[ $? -ne 0 ]]; then
    echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
    exit
  fi
   pt_now=$1
   pt=$1
fi


echo "now:  " $pt_now
echo "pt:   " $pt

shell_path="/mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_rpt_ecommerce_funnel_report"


#创建表
hive -f ${shell_path}/dwb_fd_rpt_ecommerce_funnel_report_create.hql

spark-sql \
--conf "spark.app.name=dwb_fd_rpt_ecommerce_funnel_report_lujiaheng"   \
--conf "spark.dynamicAllocation.maxExecutors=60" \
-d pt=$pt \
-f ${shell_path}/dwb_fd_rpt_ecommerce_funnel_report_insert.hql