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

#脚本路径
shell_path="/mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_goods_add_test_channel_info_rpt"

#订单事实表
#hive -hiveconf pt=pt -f ${shell_path}/dwb_fd_goods_add_test_channel_info.hql

spark-sql \
--conf "spark.app.name=goods_add_test_channel_info_yjzhang"   \
-d pt=$pt \
-f ${shell_path}/dwb_fd_goods_add_test_channel_info.hql

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "goods_add_test_channel_info rpt table is finished !"
