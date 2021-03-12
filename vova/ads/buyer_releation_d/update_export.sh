#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
--connect jdbc:mariadb:aurora://db-logistics-w.gitvv.com:3306/themis_logistics_report \
--username vvreport4vv --password nTTPdJhVp!DGv5VX4z33Fw@tHLmIG8oS --connection-manager org.apache.sqoop.manager.MySQLManager \
--table ads_user_portrait \
--update-key "user_id" \
--update-mode allowinsert \
--hcatalog-database ads \
--hcatalog-table ads_vova_buyer_releation_d \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${cur_date} \
--fields-terminated-by '\001'

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi