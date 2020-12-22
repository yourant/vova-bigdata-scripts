#!/bin/sh
if [ ! -n "$1" ] ;then
    pt=`date -d "-1 days" +%Y-%m-%d`
    pt_last=`date -d "-2 days" +%Y-%m-%d`
else
    echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $1 +%Y-%m-%d > /dev/null
    if [[ $? -ne 0 ]]; then
        echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
        exit
    fi
    pt=$1
    pt_last=`date -d "$1 -1 days" +%Y-%m-%d`

fi

#hive sql中使用的变量
echo $pt
echo $pt_last

shell_path="/mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_users_repurchase_week"

spark-sql \
--conf "spark.app.name=dwd_fd_users_repurchase_weekly_htgao"   \
--conf "spark.dynamicAllocation.initialExecutors=20"  \
--driver-memory 4g \
-d pt=$pt \
-f ${shell_path}/dwd_fd_users_repurchase_weekly.hql

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "dwd_fd_users_repurchase_weekly table is finished !"
