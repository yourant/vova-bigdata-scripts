#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--table merchant_refund_ratio_week \
--m 1 \
--update-key "mct_id,country,shipping_type,count_date" \
--update-mode allowinsert \
--hcatalog-database ads \
--hcatalog-table ads_vova_mct_refund_cnt_w \
--hcatalog-partition-keys pt \
--hcatalog-partition-values $pt \
--fields-terminated-by '\001' \
--columns "mct_id,country,shipping_type,count_date,refund_num,item_dont_fit,poor_quality,item_not_as_described,defective_item,shipment_late,wrong_product,wrong_quantity,not_receive,others,empty_package"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi