#!/bin/sh
source /mnt/vova-bigdata-scripts/common/fd_db_config.sh

#mysql配置信息
db_host=${artemis_db[db_host]}
db_port=${artemis_db[db_port]}
db_user_name=${artemis_db[db_user_name]}
db_password=${artemis_db[db_password]}
db_databases=${artemis_db[db_databases]}
db_table_name="feed_tag_log"

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
--split-by goods_id \
-m 10

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

rm -f ${db_table_name}.java
