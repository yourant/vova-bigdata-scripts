#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
--connect jdbc:mysql://db-logistics-w.gitvv.com:3306/themis_logistics_report \
--username vvreport20210517 --password thuy*at1OhG1eiyoh8she \
--table rpt_goods_restrict_d \
--update-key "goods_id,event_date" \
--update-mode allowinsert \
--hcatalog-database rpt \
--hcatalog-table rpt_goods_restrict_d \
--hcatalog-partition-keys pt \
--hcatalog-partition-values $pt \
--fields-terminated-by '\001'

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi