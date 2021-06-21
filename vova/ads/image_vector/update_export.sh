#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  hive -e "msck repair table ads.ads_vova_image_vector_target_d;"
if [ $? -ne 0 ];then
  exit 1
fi
max_pt=$(hive -e "show partitions ads.ads_vova_image_vector_target_d" | tail -1)
if [ $? -ne 0 ];then
  exit 1
fi
pt=${max_pt:3}
fi
echo "pt=$pt"

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
-Dmapreduce.map.memory.mb=12288 \
-Dmapreduce.reduce.memory.mb=12288 \
-Dsqoop.export.records.per.statement=500 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/als_images \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--update-key "vector_id" \
--m 1 \
--table ads_image_vector_v3 \
--update-mode allowinsert \
--hcatalog-database ads \
--hcatalog-table ads_vova_image_vector_target_d \
--hcatalog-partition-keys pt  \
--hcatalog-partition-values  ${pt} \
--fields-terminated-by '\001' \
--columns vector_id,img_id,goods_id,class_id,img_url,vector_base64,event_date,sku_id,cat_id,first_cat_id,second_cat_id,brand_id,is_delete,is_on_sale,is_update


if [ $? -ne 0 ];then
   exit 1
fi