#!/bin/sh
## 脚本参数注释:
## $1 日期%Y-%m-%d【非必传】

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

s3_path="s3://vova-bd-test/warehouse_test/ods"
shell_path="/mnt/vova-bigdata-scripts/fd/ods/snowplow/prc_snowplow_all_event"

#订单事实表dwd_fd_order_info
hive -hiveconf pt=$pt -hiveconf s3_path=$s3_path -f $shell_path/ods_fd_prc_snowplow_all_event.hql
