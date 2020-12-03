#!/bin/sh
home=`dirname "$0"`
cd $home

if [ ! -n "$1" ] ;then
    pt=`date -d "-1 days" +%Y-%m-%d`
    pt_last=`date -d "-2 days" +%Y-%m-%d`
    pt_format=`date -d "-1 days" +%Y%m%d`
    pt_format_last=`date -d "-2 days" +%Y%m%d`
else
    echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $1 +%Y-%m-%d > /dev/null
    if [[ $? -ne 0 ]]; then
        echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
        exit
    fi
    pt=$1
    pt_last=`date -d "$1 -1 days" +%Y-%m-%d`
    pt_format=`date -d "$1" +%Y%m%d`
    pt_format_last=`date -d "$1 -1 days" +%Y%m%d`

fi

#hive sql中使用的变量
echo $pt
echo $pt_last
echo $pt_format
echo $pt_format_last

#hive -hiveconf pt=$pt -f /mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_module_conversion_report/dwd_fd_common_module_interact.hql

shell_path="/mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_module_conversion_rpt"

spark-sql \
--conf "spark.app.name=dwd_fd_common_module_interact_yjzhang"   \
-d pt=$pt \
-f ${shell_path}/dwd_fd_common_module_interact.hql


#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "step1: common_module_interact table is finished !"

#hive -hiveconf pt=$pt -f  /mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_module_conversion_report/dwb_fd_module_conversion_rpt.hql


spark-sql \
--conf "spark.app.name=dwb_fd_module_conversion_rpt_yjzhang"   \
-d pt=$pt \
-f ${shell_path}/dwb_fd_module_conversion_rpt.hql


#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "step2: report table is finished !"
