#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
spark-sql   --conf "spark.app.name=dwb_vova_stay_coupon" --conf "spark.sql.crossJoin.enabled=true"  --conf "spark.dynamicAllocation.maxExecutors=120"  -e "

insert overwrite table dwb.dwb_vova_stay_coupon PARTITION (pt = '${cur_date}')
select
'${cur_date}',nvl(nvl(get_json_object(extra, '$.coupon_config_id'),'NA'),'all'),count(1)
from (select *,row_number() over (partition by device_id,uid order by cur_time desc) rn from ods_vova_ext.ods_vova_vvqueue_vvevent where pt = '${cur_date}' and event_type = 'new_user_coupon') t
where get_json_object(extra, '$.action') = 'request' and rn = 1
group by cube (nvl(get_json_object(extra, '$.coupon_config_id'),'NA'))
"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi