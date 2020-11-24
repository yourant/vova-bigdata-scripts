#!/bin/sh
home=`dirname "$0"`
cd $home

if [ ! -n "$1" ] ;then
    dt=`date -d "-1 days" +%Y-%m-%d`
    dt_last=`date -d "-2 days" +%Y-%m-%d`
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

#脚本路径
shell_path="/mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_main_process_rpt"

#主流程事实表
#hive -hiveconf dt=$dt -f ${shell_path}/dwb_fd_main_process_rpt.hql

spark-sql \
  --conf "spark.app.name=main_process_gaohaitao" \
  --conf "spark.dynamicAllocation.maxExecutors=60" \
  -d $pt=$pt \
  -d $pt3=$pt3 \
  -d $pt11=$pt11 \
  -f ${shell_path}/dwb_fd_main_process_rpt.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "dwb_fd_main_process_rpt table is finished !"