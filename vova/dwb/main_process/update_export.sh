#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=important \
--connect jdbc:mysql://db-logistics-w.gitvv.com:3306/themis_logistics_report \
--username vvreport4vv --password nTTPdJhVp!DGv5VX4z33Fw@tHLmIG8oS \
--table rpt_main_process \
--update-key "event_date,datasource,country,os_type,main_channel,is_new,is_brand" \
--update-mode allowinsert \
--hcatalog-database rpt \
--hcatalog-table rpt_main_process \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${cur_date} \
--fields-terminated-by '\001'

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi


