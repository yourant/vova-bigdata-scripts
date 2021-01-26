#!/bin/sh

if [ ! -n "$1" ] ;then
    pt=`date -d "-1 days" +%Y-%m-%d`
else
    echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $1 +%Y-%m-%d > /dev/null
    if [[ $? -ne 0 ]]; then
        echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
        exit
    fi
    pt=$1

fi

#hive sql中使用的变量
echo $pt

shell_path="/mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_app_retention_activity_rpt"

#计算访问积分页数据
#hive -hiveconf pt=$pt -f ${shell_path}/checkin.hql

spark-sql \
--conf "spark.app.name=dwd_fd_app_retention_activity_rpt_gaohaitao"   \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.executor.memoryOverhead=10G" \
--driver-memory 4g \
-d pt=$pt \
-f ${shell_path}/app_retention_activity_rpt.hql

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "step: app_retention_activity_rpt table is finished !"
