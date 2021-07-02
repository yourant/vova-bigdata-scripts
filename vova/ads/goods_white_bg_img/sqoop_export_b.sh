#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "${pre_date}"

hive -e "msck repair table ads.ads_vova_goods_white_bg_img_res_b;"
if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.map.memory.mb=8096 \
-Dsqoop.export.records.per.statement=500 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/als_images?disableMariaDbDriver \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--m 5 \
--table ads_vova_goods_white_bg_img_res_b \
--hcatalog-database ads \
--hcatalog-table ads_vova_goods_white_bg_img_res_b \
--columns goods_id,img_id,old_url,new_url \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--update-key "goods_id" \
--update-mode allowinsert \
--fields-terminated-by ','

if [ $? -ne 0 ];then
  exit 1
fi
