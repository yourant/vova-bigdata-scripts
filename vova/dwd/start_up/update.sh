#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###更新fact_start_up
sql="
insert overwrite table dwd.dwd_vova_fact_start_up PARTITION (pt='${cur_date}')
select /*+ REPARTITION(1) */
       su.datasource,
       su.device_id,
       su.buyer_id,
       '${cur_date}'                     start_up_date,
       su.app_version,
       su.os_type  as platform,
       su.language                             language_code,
       su.geo_country as region_code,
       su.country as app_region_code,
       from_unixtime(cast(min(collector_tstamp)/1000 as int)) as min_collector_time,
       from_unixtime(cast(max(collector_tstamp)/1000 as int)) as max_collector_time
from dwd.dwd_vova_log_screen_view su
where su.pt = '${cur_date}'
group by su.datasource, su.device_id, su.app_version, su.os_type, su.buyer_id, su.language, su.country, su.geo_country;

insert overwrite table tmp.tmp_vova_css_start_up
select su.datasource,
       su.device_id,
       su.buyer_id,
       su.start_up_date,
       su.app_version,
       su.platform,
       su.language_code,
       su.region_code,
       su.app_region_code,
       min_collector_time,
       max_collector_time
from dwd.dwd_vova_fact_start_up su
where su.pt >= date_sub('${cur_date}', 60)
    and su.datasource = 'vova';
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql  --conf "spark.app.name=dwd_vova_fact_start_up" --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.sql.output.merge=true"  --conf "spark.sql.output.coalesceNum=1" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi


