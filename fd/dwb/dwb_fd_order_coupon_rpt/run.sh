#!/bin/sh
home=`dirname "$0"`
cd $home

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
#脚本路径
shell_path="/mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_order_coupon_rpt"

hive -hiveconf pt=$pt -f ${shell_path}/dwb_fd_order_coupon.hql
