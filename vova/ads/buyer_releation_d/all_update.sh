#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

sql="
insert overwrite table ads.ads_buyer_releation
select
buyer_id user_id,
current_app_version app_version,
from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as last_update_time
from dwd.dim_buyers
where buyer_id >0 and current_app_version is not null and current_app_version !=''
"
spark-sql -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
--connect jdbc:mysql://db-logistics-w.gitvv.com:3306/themis_logistics_report \
--username vvreport4vv --password nTTPdJhVp!DGv5VX4z33Fw@tHLmIG8oS \
--table ads_user_portrait \
--update-key "user_id" \
--update-mode allowinsert \
--hcatalog-database ads \
--hcatalog-table ads_buyer_releation \
--fields-terminated-by '\001'

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi