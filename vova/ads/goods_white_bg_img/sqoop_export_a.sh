#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "${pre_date}"

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.map.memory.mb=8096 \
-Dsqoop.export.records.per.statement=1000 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/als_images?disableMariaDbDriver \
--username bdwriter --password Dd7LvXRPDP4iIJ7FfT8e \
--m 5 \
--table ads_vova_goods_white_bg_img_res_a \
--hcatalog-database ads \
--hcatalog-table ads_vova_goods_white_bg_img_res_a \
--columns goods_id,img_id,old_url,new_url \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--update-key "goods_id" \
--update-mode allowinsert \
--fields-terminated-by ','

if [ $? -ne 0 ];then
  exit 1
fi
