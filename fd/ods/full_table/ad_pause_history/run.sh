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

#shell_path
shell_path="/mnt/vova-bigdata-scripts/fd/ods/full_table"
s3_path="s3://vova-bd-test/warehouse_test/ods/fd/ods_fd_vb"
table_name="ad_pause_history"

#将全量数据放到arc表中
hive -hiveconf pt=$pt -hiveconf pt_format=$pt_format -hiveconf s3_path=$s3_path -f ${shell_path}/${table_name}/${table_name}_arc.hql
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "step1: ${table_name}_arc table is finished !"
#snapshot表
hive -hiveconf pt=$pt -hiveconf pt_format=$pt_format -hiveconf s3_path=$s3_path -f ${shell_path}/${table_name}/${table_name}_snapshot.hql
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "step2: ${table_name}_snapshot table is finished !"
