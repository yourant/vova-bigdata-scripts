#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=500 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis?rewriteBatchedStatements=true \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--update-mode allowinsert \
--m 1 \
--table ads_mct_behave_3m \
--hcatalog-database ads \
--hcatalog-table ads_mct_behave_3m \
--update-key pt,mct_id \
--columns pt,mct_id,mct_name,confirm_order_cnt_3m,confirm_order_cnt_1m,order_cnt_1m,gmv_1m,refund_rate_9w,wl_refund_rate_9w,nwl_refund_rate_9w,mct_cancel_cnt,mct_cancel_rate,mark_deliver_rate,online_rate,loss_weight_rate,exp_income,second_cat_ids,year_refund_rate,received_rate_9w,received_rate_year,delivered_time_per_60,delivered_time_per_80,delivered_time_per_90 \
--hcatalog-partition-keys pt  \
--hcatalog-partition-values  ${pre_date} \
--fields-terminated-by '\001' \
--batch

if [ $? -ne 0 ];then
  exit 1
fi
