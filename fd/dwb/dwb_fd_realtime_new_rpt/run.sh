#!/bin/sh
home=`dirname "$0"`
cd $home

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
--conf "spark.dynamicAllocation.maxExecutors=60" \
-d pt=$pt \
-d hour=$hour \
-f ${shell_path}/dwb_fd_realtime_rpt.hql


#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "realtime_gmv_orders rpt  table is finished !"

spark-submit --master yarn \
  --conf spark.executor.memory=4g \
  --conf spark.dynamicAllocation.maxExecutors=20 \
  --conf spark.app.name=alarm_system  \
  --conf spark.executor.memoryOverhead=2048 \
  --jars s3://vomkt-emr-rec/jar/vova-bd-monitor/javamail.jar \
  --class com.vova.bigdata.sparkbatch.monitor.MonitorMain s3://vomkt-emr-rec/jar/vova-bd-monitor/vova-bigdata-monitor-main.jar \
  --envFile prod --db dwb --tlb dwb_fd_realtime_new_rpt --op check_index,send_message \
  --date ${pt} --hour `echo -e $hour | sed -r 's/0*([0-9])/\1/'`
