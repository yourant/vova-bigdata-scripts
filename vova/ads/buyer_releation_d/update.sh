#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

sql="
insert overwrite table ads.ads_vova_buyer_releation_d PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
user_id,
app_version,
from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as last_update_time
from
(
select
buyer_id user_id,
app_version,
row_number() over (partition by buyer_id,datasource order by dvce_created_tstamp desc)        as rank
from dwd.dwd_vova_log_screen_view
where pt ='$cur_date' and buyer_id >0
) t1 where rank =1 and app_version is not null and app_version !=''
"
spark-sql --conf "spark.app.name=ads_vova_buyer_releation_d_zhangyin" --conf "spark.dynamicAllocation.maxExecutors=100"   -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi