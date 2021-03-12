#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
echo "pt=${pt}"
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username bdwriter --password Dd7LvXRPDP4iIJ7FfT8e \
--table goods_changed_day_record \
--update-key "goods_id,daytime" \
--update-mode allowinsert \
--hcatalog-database ads \
--hcatalog-table ads_goods_changed_record_d \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pt} \
--fields-terminated-by '\001' \
--columns "goods_id,goods_name_flag,goods_desc_flag,goods_thumb_flag,daytime"

if [ $? -ne 0 ];then
  echo "goods_changed_record_d sqoop error"
  exit 1
fi