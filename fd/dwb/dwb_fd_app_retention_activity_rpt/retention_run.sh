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

#计算留存数据
#hive -hiveconf pt=$pt -f ${shell_path}/retention.hql

spark-sql \
--conf "spark.app.name=dwd_fd_checkin_gaohaitao"   \
--conf "spark.dynamicAllocation.maxExecutors=60" \
-d pt=$pt \
-f ${shell_path}/retention.hql

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "step: retention table is finished !"
