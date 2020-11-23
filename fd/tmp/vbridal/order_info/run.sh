#!/bin/sh
source /mnt/vova-bigdata-scripts/common/fd_db_config.sh


home=`dirname "$0"`
cd $home

if [ ! -n "$1" ] ;then
    #hive 表的pt
    pt=`date -d "$pt -1 days" +%Y-%m-%d`
else
    pt=$1
fi

pt_format=$(date -d "-1 day" +%Y%m%d)

echo $pt
echo $pt_format

#mysql配置信息
db_host=${vbridal_db[db_host]}
db_port=${vbridal_db[db_port]}
db_user_name=${vbridal_db[db_user_name]}
db_password=${vbridal_db[db_password]}
db_databases=${vbridal_db[db_databases]}
db_table_name="order_info"

hive_db_name="tmp_fd_vb"

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
-m 5

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

rm -r *.java
