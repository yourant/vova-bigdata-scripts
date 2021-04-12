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
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/als_images?tinyInt1isBit=false \
--username bdwriter --password Dd7LvXRPDP4iIJ7FfT8e \
--table ads_vova_goods_img_enhance \
--update-key "img_id" \
--update-mode allowinsert \
--hcatalog-database ads \
--hcatalog-table ads_vova_img_enhance_result_d \
--hcatalog-partition-keys pt \
--hcatalog-partition-values $pt \
--fields-terminated-by ',' \
--columns "goods_id,img_id,img_url_aws,img_url_gcs,is_default"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi