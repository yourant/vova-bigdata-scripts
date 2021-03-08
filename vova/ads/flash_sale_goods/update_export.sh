#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "0 day" +%Y/%m/%d/%H`
echo "cur_date=${cur_date}"
fi
pt=`date -d "0 day" +%Y-%m-%d`
echo "pt=$pt"

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username bdwriter --password Dd7LvXRPDP4iIJ7FfT8e \
--table ads_flash_sale_goods \
--update-key "goods_id,region_id,rank,flash_sale_date,event_type" \
--update-mode allowinsert \
--hcatalog-database ads \
--hcatalog-table ads_flash_sale_goods_d \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pt} \
--fields-terminated-by '\001' \
--columns "goods_id,second_cat_id,flash_sale_date,region_id,event_type,rank"

if [ $? -ne 0 ];then
  echo "ads_flash_sale_goods_d sqoop error"
  exit 1
fi

sh /mnt/vova-bd-scripts/ods/job_message_put.sh --jname=ads_flash_sale_task --from=data --to=rec_service --jtype=1D --retry=0

if [ $? -ne 0 ];then
  exit 1
fi