#!/bin/sh
home=`dirname "$0"`
cd $home

if [ ! -n "$1" ] ;then
    pt=`date -d "-1 days" +%Y-%m-%d`
    pt_last=`date -d "-2 days" +%Y-%m-%d`
    dt_format=`date -d "-1 days" +%Y%m%d`
    dt_format_last=`date -d "-2 days" +%Y%m%d`
else
    echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $1 +%Y-%m-%d > /dev/null
    if [[ $? -ne 0 ]]; then
        echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
        exit
    fi
    dt=$1
    dt_last=`date -d "$1 -1 days" +%Y-%m-%d`
    dt_format=`date -d "$1" +%Y%m%d`
    dt_format_last=`date -d "$1 -1 days" +%Y%m%d`

fi

#hive sql中使用的变量
echo $dt
echo $dt_last
echo $dt_format
echo $dt_format_last

shell_path="/mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_banner_ctr_rpt"

#计算留存数据
#hive -hiveconf dt=$dt -f ${shell_path}/dwb_fd_banner_ctr_rpt.hql


spark-sql \
--conf "spark.app.name=dwb_fd_common_ctr_rpt_yjzhang"   \
--conf "spark.dynamicAllocation.maxExecutors=60" \
-d pt=$pt \
-f ${shell_path}/dwb_fd_banner_ctr_rpt.hql


#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "banner report  table is finished !"
