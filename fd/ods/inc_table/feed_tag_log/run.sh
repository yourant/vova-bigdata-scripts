#!/bin/sh
source /mnt/vova-bigdata-scripts/common/fd_db_config.sh

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

#mysql配置信息
db_host=${artemis_db[db_host]}
db_port=${artemis_db[db_port]}
db_user_name=${artemis_db[db_user_name]}
db_password=${artemis_db[db_password]}
db_databases=${artemis_db[db_databases]}
db_table_name="feed_tag_log"

######hive配置信息
hive_db_name="tmp"
hive_columns="feed_name ,goods_id ,log_date ,version_id ,virtual_goods_id ,cat_id ,product_type ,goods_thumb ,ads_grouping ,adwords_labels ,custom_label_0 ,custom_label_1 ,custom_label_2 ,custom_label_3 ,custom_label_4 ,last_update_time"
#hive tmp表在s3的路径
db_info=`hive -e "describe database tmp;"`
s3_hive_path=`echo $db_info |awk -F" "  '{print $2}'`

#脚本路径
shell_path="/mnt/vova-bigdata-scripts/fd/ods/inc_table/${db_table_name}"

#map task并发
hive_mappers="4"
#增量字段
inc_column="last_update_time"
#主键
primary_key="goods_id"

sqoop import \
--connect "jdbc:mysql://${db_host}:${db_port}/${db_databases}?tinyInt1isBit=false" \
--username ${db_user_name} \
--password ${db_password} \
--query "select ${hive_columns} from ${db_table_name} where ${inc_column} >= '${pt} 00:00:00' and ${inc_column} <= '${pt} 23:59:59' and \$CONDITIONS" \
--target-dir ${s3_hive_path}${pt} \
--delete-target-dir \
--hive-import \
--hive-overwrite \
--hive-database ${hive_db_name} \
--hive-table tmp_fd_${db_table_name} \
--hive-partition-key pt \
--hive-partition-value ${pt} \
--fields-terminated-by '\001' \
--split-by ${primary_key} \
-m ${hive_mappers}

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "step1: ${db_table_name} table is finished !"
rm -r *.java

#inc 表
hive -hiveconf pt=$pt -f ${shell_path}/${db_table_name}_inc.hql

if [ $? -ne 0 ];then
  exit 1
fi
echo "step2: ${db_table_name}_inc table is finished !"

#arc 表
#hive -hiveconf pt=$pt -hiveconf pt_last=$pt_last -f ${shell_path}/${db_table_name}_arc.hql

#arc 全量补全表
hive -hiveconf pt=$pt -f ${shell_path}/${db_table_name}_arc_full.hql

if [ $? -ne 0 ];then
  exit 1
fi
echo "step3: ${db_table_name}_arc table is finished !"

#snapshot表
hive -hiveconf pt=$pt -f ${shell_path}/${db_table_name}_snapshot.hql

if [ $? -ne 0 ];then
  exit 1
fi
echo "step4: ${db_table_name}_snapshot table is finished !"
