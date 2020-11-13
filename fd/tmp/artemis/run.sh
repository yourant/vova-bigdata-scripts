#!/bin/sh

## 脚本参数注释:
## $1 表名【必传】
## $2 启动map task个数【非必传】
source /mnt/vova-bigdata-scripts/common/fd_db_config.sh
#source /mnt/vova-bigdata-scripts/fd/tmp/common/fd_db_config.sh

if [[ $# -lt 1 ]]; then
	echo "脚本必传一个参数，该参数代表是要执行的表名 【字符串类型】!"
    	exit 1

elif [[ $# -ge 1 && $# -le 2 ]]; then
	echo $1 | grep "[a-zA-Z]" > /dev/null
	if [[ $? -eq 1 ]]; then
                echo "第一个参数[ $1 ]不符合要执行的表名, 请输入正确的表名!"
                exit 1
        fi
	db_table_name=$1
	
	if [[ $# -ge 2 ]]; then
		echo $2 | grep -q '[^0-9]' > /dev/null
        	if [[ $? -eq 0 ]]; then
                	echo "第二个参数[ $2 ]不符合数值类型, 请输入你要执行的Map Task的个数!"
                	exit 1
        	fi
		hive_map_tasks=$2	

	else
		hive_map_tasks=5
	fi
fi

echo "当前执行的表名："$db_table_name , "开启的任务数："$hive_map_tasks

#获取数据库配置信息
db_host=${artemis_db[db_host]}
db_port=${artemis_db[db_port]}
db_user_name=${artemis_db[db_user_name]}
db_password=${artemis_db[db_password]}
db_databases=${artemis_db[db_databases]}
#hive数据库
hive_db_name="tmp"

sqoop import \
--connect "jdbc:mysql://${db_host}:${db_port}/${db_databases}?tinyInt1isBit=false" \
--username ${db_user_name} \
--password ${db_password} \
--table ${db_table_name} \
--hive-import \
--hive-overwrite \
--hive-database ${hive_db_name} \
--hive-table tmp_fd_${db_table_name}_full \
--direct \
--fields-terminated-by '\001' \
--null-string '\\N' \
--null-non-string '\\N' \
--delete-target-dir \
-m ${hive_map_tasks}

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

rm -f ${db_table_name}.java
