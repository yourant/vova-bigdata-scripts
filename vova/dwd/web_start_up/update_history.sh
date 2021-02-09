#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###更新fact_start_up

sql="
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=1000;
INSERT OVERWRITE TABLE dwd.dwd_vova_fact_web_start_up PARTITION (pt)
SELECT
/*+ REPARTITION(1) */
datasource,
domain_userid,
buyer_id,
region_code,
first_page_url,
first_referrer,
min_create_time,
max_create_time,
pt
from
(SELECT log.datasource,
       log.domain_userid,
       log.buyer_id,
       log.geo_country                                             AS region_code,
       row_number() OVER (PARTITION BY datasource, buyer_id, domain_userid, geo_country, pt ORDER BY pt DESC, dvce_created_tstamp DESC) AS rank,
       first_value(page_url) OVER (PARTITION BY datasource, buyer_id, domain_userid, geo_country, pt ORDER BY pt, dvce_created_tstamp) AS first_page_url,
       first_value(referrer) OVER (PARTITION BY datasource, buyer_id, domain_userid, geo_country, pt ORDER BY pt, dvce_created_tstamp) AS first_referrer,
       first_value(from_unixtime(cast(dvce_created_tstamp / 1000 AS int))) OVER (PARTITION BY datasource, buyer_id, domain_userid, geo_country, pt ORDER BY pt, dvce_created_tstamp) AS min_create_time,
       from_unixtime(cast(dvce_created_tstamp / 1000 AS int)) AS max_create_time,
       log.pt
FROM dwd.dwd_vova_log_page_view log
WHERE log.pt >= '2020-06-10'
  AND (log.dp = 'airyclub' OR log.dp = 'vova')
  AND log.platform in ('web', 'pc')) su
WHERE su.rank = 1
;


set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=1000;
INSERT OVERWRITE TABLE tmp.tmp_vova_web_main_process_register PARTITION (pt)
SELECT /*+ REPARTITION(1) */
       log.datasource,
       log.domain_userid,
       min(dvce_created_tstamp) AS min_create_time,
       log.pt
FROM dwd.dwd_vova_log_data log
WHERE log.datasource = 'airyclub'
  AND log.platform IN ('web', 'pc')
  AND log.element_name = 'register_success'
  AND log.event_name = 'data'
  AND log.pt >= '2020-06-10'
GROUP BY log.pt, log.datasource, log.domain_userid
;
"
