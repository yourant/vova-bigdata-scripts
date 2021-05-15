#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "0 day" +%Y-%m-%d`
pre_2w=`date -d "15 day ago ${pt}" +%Y-%m-%d`
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--table ads_gsn_reduce_valid_goods \
--m 1 \
--update-key "goods_id,add_cycle" \
--update-mode allowinsert \
--hcatalog-database ads \
--hcatalog-table ads_vova_gsn_reduce_valid_goods \
--hcatalog-partition-keys pt \
--hcatalog-partition-values $pt \
--fields-terminated-by '\001' \
--columns "goods_id,add_cycle,expre,sales_order,expre_cr"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi