#!/bin/sh
## 脚本参数注释:
## $1 表名【必传】
## $2 日期 %Y-%m-%d【非必传】

if [[ $# -lt 1 ]]; then
        echo "脚本必传一个参数，该参数代表是要执行的表名 【字符串类型】!"
        exit 1

elif [[ $# -ge 1 && $# -le 2 ]]; then
        echo $1 | grep "[a-zA-Z]" > /dev/null
        if [[ $? -eq 1 ]]; then
                echo "第一个参数[ $1 ]不符合要执行的表名, 请输入正确的表名!"
                exit 1
        fi
        table_name=$1
	pt=`date -d "-1 days" +%Y-%m-%d`
	pt_last=`date -d "-2 days" +%Y-%m-%d`
	pt_format=`date -d "-1 days" +%Y%m%d`
	pt_format_last=`date -d "-2 days" +%Y%m%d`

        if [[ $# -eq 2 ]]; then
		echo $2 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $2 +%Y-%m-%d > /dev/null
		if [[ $? -ne 0 ]]; then
			echo "接收的第二个参数【${2}】不符合:%Y-%m-%d 时间格式，请输入正确的格式!"
			exit 1
		fi
		table_name=$1
		pt=$2
    		pt_last=`date -d "$2 -1 days" +%Y-%m-%d`
		pt_fcormat=`date -d "$2" +%Y%m%d`
		pt_format_last=`date -d "$2 -1 days" +%Y%m%d`
	fi
fi

#hive sql中使用的变量
echo $table_name
echo $pt
echo $pt_last
echo $pt_format
echo $pt_format_last

#shell_path
shell_path="/mnt/vova-bigdata-scripts/fd/ods/full_table"

#将全量数据放到arc表中
hive -hiveconf pt=$pt -hiveconf pt_format=$pt_format -f ${shell_path}/${table_name}/${table_name}_arc.hql
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "step1: ${table_name}_arc table is finished !"

#snapshot表
hive -hiveconf pt=$pt -hiveconf pt_format=$pt_format -f ${shell_path}/${table_name}/${table_name}_snapshot.hql
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "step2: ${table_name}_snapshot table is finished !"
