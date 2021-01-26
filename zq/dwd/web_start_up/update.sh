#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###更新fact_web_start_up
sql="
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE dwd.dwd_zq_fact_web_start_up PARTITION (pt)
SELECT
/*+ REPARTITION(2) */
datasource,
domain_userid,
buyer_id,
platform,
region_code,
first_page_url,
first_referrer,
min_create_time,
max_create_time,
pt
from
(SELECT log.datasource,
       log.domain_userid,
       log.platform,
       log.buyer_id,
       log.geo_country                                             AS region_code,
       row_number() OVER (PARTITION BY log.datasource, buyer_id, domain_userid, geo_country,platform, pt ORDER BY pt DESC, dvce_created_tstamp DESC) AS rank,
       first_value(page_url) OVER (PARTITION BY log.datasource, buyer_id, domain_userid, geo_country,platform, pt ORDER BY pt, dvce_created_tstamp) AS first_page_url,
       first_value(referrer) OVER (PARTITION BY log.datasource, buyer_id, domain_userid, geo_country,platform, pt ORDER BY pt, dvce_created_tstamp) AS first_referrer,
       first_value(dvce_created_ts) OVER (PARTITION BY log.datasource, buyer_id, domain_userid, geo_country, platform, pt ORDER BY pt, dvce_created_tstamp) AS min_create_time,
       dvce_created_ts AS max_create_time,
       log.pt
FROM dwd.dwd_vova_log_page_view log
inner join dim.dim_zq_site se on se.datasource = log.datasource
WHERE log.pt = '${cur_date}'
  AND log.dp = 'others'
  AND log.platform in ('web', 'pc')) su
WHERE su.rank = 1
;

INSERT OVERWRITE TABLE dwd.dwd_zq_fact_original_channel_daily PARTITION (pt)
select
/*+ REPARTITION(2) */
t1.datasource,
t1.domain_userid,
if(t3.original_channel is not null,t3.original_channel, 'unknown') AS original_channel,
if(t3.original_channel is not null,t3.dvce_created_ts, t1.dvce_created_ts) AS dvce_created_ts,
t1.pt
from
(
select
log.datasource,
log.domain_userid,
log.pt,
min(log.dvce_created_ts) as dvce_created_ts
FROM dwd.dwd_vova_log_page_view log
inner join dim.dim_zq_site se on se.datasource = log.datasource
WHERE log.pt = '${cur_date}'
  AND log.dp = 'others'
  AND log.platform in ('web', 'pc')
group by log.datasource, log.domain_userid, log.pt
) t1 left join
(
select
datasource,
domain_userid,
pt,
original_channel,
dvce_created_ts
from
(
select
datasource,
domain_userid,
pt,
original_channel,
dvce_created_ts,
row_number() OVER (PARTITION BY datasource, domain_userid, pt ORDER BY dvce_created_ts ASC) AS rank
from
(
select
log.datasource,
log.domain_userid,
log.pt,
CASE
    WHEN log.referrer LIKE '%newsletter%'
        THEN 'newsletter'
    WHEN log.referrer LIKE '%facebook%'
        THEN 'facebook'
    ELSE 'unknown' END AS original_channel,
log.dvce_created_ts
FROM dwd.dwd_vova_log_page_view log
inner join dim.dim_zq_site se on se.datasource = log.datasource
WHERE log.pt = '${cur_date}'
  AND log.dp = 'others'
  AND log.platform in ('web', 'pc')
) t1
where t1.original_channel != 'unknown'
) t2
where t2.rank = 1
) t3 on t1.datasource = t3.datasource and t1.domain_userid = t3.domain_userid AND t1.pt = t3.pt
;

INSERT OVERWRITE TABLE dwd.dwd_zq_fact_original_channel
select
/*+ REPARTITION(2) */
f1.datasource,
f1.domain_userid,
f1.original_channel,
f1.dvce_created_tstamp,
f1.pt
from
(
select
t1.datasource,
t1.domain_userid,
t1.original_channel,
t1.dvce_created_tstamp,
t1.pt,
row_number() OVER (PARTITION BY t1.datasource, t1.domain_userid ORDER BY t1.pt ASC, t1.dvce_created_tstamp ASC) AS rank
from
dwd.dwd_zq_fact_original_channel_daily t1
) f1
where f1.rank = 1

;
"

#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=dwd.dwd_zq_fact_web_start_up" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi