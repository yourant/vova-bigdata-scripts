#!/bin/sh
home=`dirname "$0"`
cd $home

if [ ! -n "$1" ] ;then
    dt_now=`date +%Y-%m-%d`
    dt_now_format=`date +%Y%m%d`
    dt=`date -d "-1 days" +%Y-%m-%d`
    dt_format=`date -d "-1 days" +%Y%m%d`
else
    echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $1 +%Y-%m-%d > /dev/null
    if [[ $? -ne 0 ]]; then
        echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
        exit
    fi
    dt_now=$1
    dt_now_format=`date -d "$1" +%Y%m%d`
    dt=`date -d "$1 -1 days" +%Y-%m-%d`
    dt_format=`date -d "$1 -1 days" +%Y%m%d`

fi

#hive sql中使用的变量
echo $dt_now
echo $dt_now_format
echo $dt
echo $dt_format

shell_path="/mnt/vova-bigdata-scripts/fd/ods/binlog_table"
s3_path="s3://vova-bd-test/warehouse_test/ods/fd/ods_fd_vb"
#flume搜集的binlog日志路径
flume_path="s3a://vova-bd-test/flume/fd/vbridal"
table_name="order_info"

#将flume收集的数据存到tmp表中
hive -hiveconf flume_path=$flume_path -f ${shell_path}/${table_name}/tmp_${table_name}.hql

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
hive -hiveconf dt=$dt -hiveconf s3_path=$s3_path -f ${shell_path}/${table_name}/${table_name}_arc_full.hql

#arc最终表
#hive -hiveconf dt=$dt -hiveconf dt_last=$dt_last  -hiveconf s3_path=$s3_path -f ${shell_path}/${table_name}/${table_name}_arc.hql

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
