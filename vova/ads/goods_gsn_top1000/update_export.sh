#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username bdwriter --password Dd7LvXRPDP4iIJ7FfT8e \
--m 1 \
--table gsn_top1000 \
--hcatalog-database ads \
--hcatalog-table ads_vova_gsn_top1000 \
--update-mode allowinsert \
--update-key gs_id,pt,ctry,rec_page_code \
--hcatalog-partition-keys pt  \
--hcatalog-partition-values  ${cur_date} \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi