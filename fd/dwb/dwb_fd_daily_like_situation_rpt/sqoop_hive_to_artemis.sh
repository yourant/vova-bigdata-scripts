#!/bin/sh
if [ ! -n "$1" ] ;then
    pt=`date -d "-1 days" +%Y-%m-%d`
    pt3=`date -d "-4 days" +%Y-%m-%d`
    pt11=`date -d "-12 days" +%Y-%m-%d`
else
    echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $1 +%Y-%m-%d > /dev/null
    if [[ $? -ne 0 ]]; then
        echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
        exit
    fi
    pt=$1
    pt3=`date -d "$1 -3 days" +%Y-%m-%d`
    pt11=`date -d "$1 -11 days" +%Y-%m-%d`

fi

#hive sql中使用的变量
echo $pt
echo $pt3
echo $pt11

#脚本路径
shell_path="/mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_main_process_rpt"

#打版数据
sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
--connect jdbc:mysql://artemis-data.cpbbe5ehgjpf.us-east-1.rds.amazonaws.com:3306/data_report \
--username data-report --password C27PoowhAZIU$LHeI%Gs \
--table daily_like_situation_rpt \
--update-key "pt, batch, virtual_goods_id, project, country, platform_type" \
--update-mode allowinsert \
--hcatalog-database dwb \
--hcatalog-table dwb_fd_daily_like_situation_rpt \
--hcatalog-partition-keys pt \
--hcatalog-partition-values 2020-11-29 \
--fields-terminated-by '\001'

if [ $? -ne 0 ]; then
  exit 1
fi
echo "dwb_fd_main_process_rpt table is finished !"