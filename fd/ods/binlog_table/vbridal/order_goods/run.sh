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

flume_path="s3://bigdata-offline/warehouse/pdb/fd/vbridal"
shell_path="/mnt/vova-bigdata-scripts/fd/ods/binlog_table"
table_name="order_goods"

#将flume收集的数据存到tmp表中
hive -hiveconf flume_path=$flume_path -f  ${shell_path}/${table_name}/tmp_${table_name}.hql

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "step1: tmp_${table_name} table is finished !"

#将每天增量数据放到inc对应的天表中
hive -hiveconf dt=$dt -hiveconf s3_path=$s3_path -f ${shell_path}/${table_name}/${table_name}_inc.hql

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "step2: ${table_name}_inc table is finished !"

#这一步为了初始化订单表，将全量数据放到arc表中
#hive -hiveconf dt=$dt -hiveconf s3_path=$s3_path -f ${shell_path}/${table_name}/${table_name}_arc_full.hql

#arc最终表
hive -hiveconf dt=$dt -hiveconf dt_last=$dt_last  -hiveconf s3_path=$s3_path -f ${shell_path}/${table_name}/${table_name}_arc.hql

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "step3: ${table_name}_arc table is finished !"

#snapshot表
hive -hiveconf dt=$dt -hiveconf s3_path=$s3_path -f ${shell_path}/${table_name}/${table_name}_snapshot.hql

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "step4: ${table_name}_snapshot table is finished !"
