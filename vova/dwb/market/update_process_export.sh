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
-Dsqoop.export.records.per.statement=500 \
-Dmapreduce.job.queuename=important \
--connect jdbc:mysql://db-logistics-w.gitvv.com:3306/themis_logistics_report?rewriteBatchedStatements=true  \
--username vvreport4vv --password nTTPdJhVp!DGv5VX4z33Fw@tHLmIG8oS \
--table dwb_vova_market_process \
--update-key event_date,datasource,region_code \
--update-mode allowinsert \
--m 1 \
--hcatalog-database dwb \
--hcatalog-table dwb_vova_market_process \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${cur_date} \
--fields-terminated-by '\001'

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
