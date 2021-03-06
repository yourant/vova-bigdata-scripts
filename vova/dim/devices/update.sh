#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###更新设备首单
### 取设备当前版本，当前的buyer_id，激活日期
### 更新设备维度
hadoop fs -mkdir s3://bigdata-offline/warehouse/tmp/tmp_vova_device_first_pay
hadoop fs -mkdir s3://bigdata-offline/warehouse/tmp/tmp_vova_device_app_version
hadoop fs -mkdir s3://bigdata-offline/warehouse/dim/dim_vova_devices
sql="
insert overwrite table tmp.tmp_vova_device_first_pay
SELECT /*+ REPARTITION(4) */ datasource,
       device_id,
       min(order_id) AS first_order_id,
       min(order_time)  AS first_order_time,
       min(pay_time)    AS first_pay_time
FROM (
         SELECT oi.project_name  AS datasource,
                ore.device_id,
                oi.order_id,
                oi.order_time,
                oi.pay_time
         FROM ods_vova_vts.ods_vova_order_info oi
                  INNER JOIN ods_vova_vts.ods_vova_order_relation ore ON oi.order_id = ore.order_id
         WHERE oi.pay_status >= 1
           and oi.email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
           and oi.parent_order_id = 0
           AND ore.device_id IS NOT NULL) temp
GROUP BY device_id, datasource;


insert overwrite table tmp.tmp_vova_device_app_version
select /*+ REPARTITION(40) */ datasource,
       device_id,
       app_version,
       buyer_id,
       platform,
       region_code,
       app_region_code,
       activate_time
from (select device_id,
             datasource,
             app_version,
             buyer_id,
             platform,
             region_code,
             app_region_code,
             row_number() over (partition by device_id,datasource order by pt desc, max_collector_time desc)        as rank,
             first_value(min_collector_time) over (partition by device_id,datasource order by pt, min_collector_time ) as activate_time
      from dwd.dwd_vova_fact_start_up su) su
where su.device_id is not null
  and su.rank = 1;

with tmp_dev as (
SELECT
       dav.datasource,
       if(dav.device_id IS NULL, ar.device_id, dav.device_id) AS device_id,
       cm.main_channel,
       ar.media_source as child_channel,
       if(dav.platform IS NULL, ar.platform, dav.platform) AS platform,
       ar.idfv,
       ar.android_id,
       ar.imei,
       ar.advertising_id,
       ar.http_referrer,
       ar.campaign,
       ar.os_version,
       if(dav.region_code IS NULL, ar.country_code, dav.region_code) AS region_code,
       if(dav.app_region_code IS NULL, ar.country_code, dav.app_region_code) AS app_region_code,
       ar.language                                           AS language_code,
       ar.install_time,
       ar.app_version                                        AS install_app_version,
       dav.app_version                                       AS current_app_version,
       dav.buyer_id                                          as current_buyer_id,
       dav.activate_time,
       fp.first_order_id,
       fp.first_order_time,
       fp.first_pay_time
FROM tmp.tmp_vova_device_app_version dav
LEFT JOIN ods_vova_vtlr.ods_vova_appsflyer_record_merge ar on dav.datasource = ar.datasource and dav.device_id = ar.device_id
LEFT JOIN ods_vova_vtlr.ods_vova_channel_mapping cm ON cm.child_channel = ar.media_source
LEFT JOIN tmp.tmp_vova_device_first_pay fp ON fp.device_id = dav.device_id AND fp.datasource = dav.datasource
)

INSERT overwrite TABLE dim.dim_vova_devices
select
/*+ REPARTITION(10) */
t.datasource,
t.device_id,
cm.main_channel,
ar.media_source as child_channel,
t.platform,
t.idfv,
t.android_id,
t.imei,
t.advertising_id,
t.http_referrer,
t.campaign,
t.os_version,
t.region_code,
t.app_region_code,
t.language_code,
t.install_time,
t.install_app_version,
t.current_app_version,
t.current_buyer_id,
t.activate_time,
t.first_order_id,
t.first_order_time,
t.first_pay_time
from tmp_dev t
LEFT JOIN (select datasource,idfv,media_source from (select datasource,idfv,media_source,row_number() over (partition by idfv,datasource order by install_time desc) as rank from ods_vova_vtlr.ods_vova_appsflyer_record_merge) where rank =1 ) ar on t.datasource = ar.datasource and t.device_id = ar.idfv
LEFT JOIN ods_vova_vtlr.ods_vova_channel_mapping cm ON cm.child_channel = ar.media_source
where t.main_channel is null
union all
select
 /*+ REPARTITION(60) */
datasource,
device_id,
main_channel,
child_channel,
platform,
idfv,
android_id,
imei,
advertising_id,
http_referrer,
campaign,
os_version,
region_code,
app_region_code,
language_code,
install_time,
install_app_version,
current_app_version,
current_buyer_id,
activate_time,
first_order_id,
first_order_time,
first_pay_time
from tmp_dev where main_channel is not null;
"
spark-sql --executor-memory 6G  --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.app.name=dim_vova_devices"  --conf "spark.sql.output.merge=true"  --conf "spark.sql.output.coalesceNum=50" --conf "spark.dynamicAllocation.minExecutors=30" --conf "spark.dynamicAllocation.initialExecutors=60" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

