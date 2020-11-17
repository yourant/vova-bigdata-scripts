#!/bin/sh

## 脚本参数注释:
## $1 表名【必传】
## $2 执行时间【非必传,不传时间默认执行前一天】

if [ ! -n "$1" ];then
	echo "脚本必须传一个参数,表示要执行的表名!"
	exit 1

elif [[ $# -eq 1 ]]; then
	echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $1 +%Y-%m-%d > /dev/null
	if [[ $? -eq 0 ]]; then
		echo "第一个参数不符合要执行的表名:$1 请输入正确的格式!"
		exit 1
  	fi

	echo $1 | grep -Eq "[0-9]{4}[0-9]{2}[0-9]{2}" && date -d $1 +%Y%m%d > /dev/null
	if [[ $? -eq 0 ]]; then
		echo "第一个参数不符合要执行的表名:$1 请输入正确的格式!"
		exit 1
	fi

	table_name=$1
	pt=`date -d "-1 days" +%Y-%m-%d`
  pt_last=`date -d "-2 days" +%Y-%m-%d`
  pt_format=`date -d "-1 days" +%Y%m%d`
  pt_format_last=`date -d "-2 days" +%Y%m%d`


elif [[ $# -eq 2 ]]; then
	echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $1 +%Y-%m-%d > /dev/null
	if [[ $? -eq 0 ]]; then
		echo "第一个参数不符合要执行的表名:$1 请输入正确的格式!"
		exit 1
	fi

	echo $1 | grep -Eq "[0-9]{4}[0-9]{2}[0-9]{2}" && date -d $1 +%Y%m%d > /dev/null
	if [[ $? -eq 0 ]]; then
		echo "第一个参数不符合要执行的表名:$1 请输入正确的格式!"
		exit 1
	fi

	table_name=$1

	echo $2 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $2 +%Y-%m-%d > /dev/null
	if [[ $? -ne 0 ]]; then
		echo "第二个参数时间${2}不符合:%Y-%m-%d 请输入正确的格式!"
	exit
	fi

	pt=$2
	pt_last=`date -d "$2 -1 days" +%Y-%m-%d`
	pt_format=`date -d "$2" +%Y%m%d`
	pt_format_last=`date -d "$2 -1 days" +%Y%m%d`

fi

#hive sql中使用的变量
echo $table_name
echo $pt
echo $pt_last
echo $pt_format
echo $pt_format_last

#脚本路径
shell_path="/mnt/vova-bigdata-scripts/fd/dim"

#将dim层维表
hive -hiveconf pt=$pt -f ${shell_path}/${table_name}/${table_name}.hql
