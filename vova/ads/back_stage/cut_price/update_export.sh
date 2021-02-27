#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pre_date=`date -d "-1 day" +%Y-%m-%d`
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username bimaster --password sYG2Ri3yIDu2NPki \
--table ads_goods_sn_cut_price \
--update-key "goods_sn,event_date" \
--update-mode allowinsert \
--hcatalog-database ads \
--hcatalog-table ads_goods_sn_cut_price \
--hcatalog-partition-keys pt \
--hcatalog-partition-values $pre_date \
--fields-terminated-by '\001' \
--columns "goods_sn,event_date"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi