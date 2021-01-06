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
sql="
insert overwrite table tmp.tmp_vova_device_first_pay
SELECT datasource,
       device_id,
       min(order_id) AS first_order_id,
       min(order_time)  AS first_order_time,
       min(pay_time)    AS first_pay_time
FROM (
         SELECT CASE
                    WHEN oi.from_domain LIKE '%vova%' THEN 'vova'
                    WHEN oi.from_domain LIKE '%airyclub%' THEN 'airyclub'
                    END          AS datasource,
                ore.device_id,
                oi.order_id,
                oi.order_time,
                oi.pay_time
         FROM ods_vova_vts.ods_vova_order_info oi
                  INNER JOIN ods_vova_vts.ods_vova_order_relation ore ON oi.order_id = ore.order_id
         WHERE oi.pay_status >= 1
           AND ore.device_id IS NOT NULL) temp
GROUP BY device_id, datasource;


insert overwrite table tmp.tmp_vova_device_app_version
select datasource,
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

INSERT overwrite TABLE dim.dim_vova_devices
SELECT 'vova'                                                AS datasource,
       if(ar.device_id IS NULL, dav.device_id, ar.device_id) AS device_id,
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
       ar.device_brand,
       ar.device_model,
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
       fp.first_pay_time,
       ar.click_url as clk_url
FROM ods_vova_vtl.ods_vova_appsflyer_record ar
         LEFT JOIN ods_vova_vtlr.ods_vova_channel_mapping cm ON cm.child_channel = ar.media_source
         LEFT JOIN tmp.tmp_vova_device_first_pay fp ON fp.device_id = ar.device_id AND fp.datasource = 'vova'
    FULL JOIN (SELECT device_id
   , app_version
   , platform
   , region_code
   , buyer_id
   , app_region_code
   , activate_time FROM tmp.tmp_vova_device_app_version WHERE datasource = 'vova') dav
ON dav.device_id = ar.device_id
UNION
SELECT 'airyclub'                                            AS datasource,
       if(ar.device_id IS NULL, dav.device_id, ar.device_id) AS device_id,
       ar.media_source as main_channel,
       ar.media_source as child_channel,
       if(dav.platform IS NULL, ar.platform, dav.platform) AS platform,
       ar.idfv,
       ar.android_id,
       ar.imei,
       ar.advertising_id,
       ar.http_referrer,
       ar.campaign,
       ar.os_version,
       ar.device_brand,
       ar.device_model,
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
       fp.first_pay_time,
       ar.click_url as clk_url
FROM ods_ac_acl.ods_ac_appsflyer_record ar
         LEFT JOIN ods_vova_vtlr.ods_vova_channel_mapping cm ON cm.child_channel = ar.media_source
         LEFT JOIN tmp.tmp_vova_device_first_pay fp ON fp.device_id = ar.device_id AND fp.datasource = 'airyclub'
    FULL JOIN (SELECT device_id
   , app_version
   , platform
   , region_code
   , app_region_code
   , buyer_id
   , activate_time FROM tmp.tmp_vova_device_app_version WHERE datasource = 'airyclub') dav
ON dav.device_id = ar.device_id;
"
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.app.name=dim_vova_devices"  --conf "spark.sql.output.merge=true"  --conf "spark.sql.output.coalesceNum=50" --conf "spark.dynamicAllocation.minExecutors=30" --conf "spark.dynamicAllocation.initialExecutors=60" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

