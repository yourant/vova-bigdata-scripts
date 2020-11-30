#!/bin/sh
source /mnt/vova-bigdata-scripts/common/fd_db_config.sh

home=`dirname "$0"`
cd $home

if [ ! -n "$1" ] ;then
    #hive 表的dt
    dt=`date -d "$dt -1 days" +%Y-%m-%d`
else
    dt=$1
fi

pt=$(date -d "-1 day" +%Y-%m-%d)

one_begin=`date -d "$dt -1 month" +%Y-%m-01`
one_end=`date -d "$dt -1 month" +%Y-%m-31`

echo $dt
echo $one_begin
echo $one_end

#mysql配置信息
db_host="artemis-data.cpbbe5ehgjpf.us-east-1.rds.amazonaws.com"
db_port="3306"
db_user_name="data-report"
db_password='C27PoowhAZIU$LHeI%Gs'
db_databases="data-report"
db_table_name="rpt_main_process"

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
-m 2

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

rm -r *.java
