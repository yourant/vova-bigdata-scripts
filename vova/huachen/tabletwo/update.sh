#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
cur_month=${cur_date:0:7}
echo "cur_date:'${cur_date}',cur_month:'${cur_month}'"

sql="
with tmp_total_order as (

)
"
