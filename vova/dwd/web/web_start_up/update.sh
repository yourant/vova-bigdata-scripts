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

INSERT OVERWRITE TABLE dwd.dwd_vova_fact_original_channel_daily PARTITION (pt)
select
/*+ REPARTITION(1) */
t1.datasource,
t1.domain_userid,
t1.geo_country as region_code,
t1.platform,
if(t3.domain_userid is not null,t3.original_channel, t1.original_channel) AS original_channel,
t1.dvce_created_ts,
nvl(t2.buyer_id, 0) AS buyer_id,
t1.pt
from
(
select
datasource,
domain_userid,
geo_country,
platform,
pt,
'unknown' AS original_channel,
dvce_created_ts
from
(
select
log.datasource,
log.domain_userid,
log.geo_country,
log.platform,
log.pt,
log.dvce_created_ts,
row_number() OVER (PARTITION BY log.datasource, log.domain_userid, log.pt ORDER BY dvce_created_tstamp ASC) AS rank
FROM dwd.dwd_vova_log_page_view log
WHERE log.pt = '${cur_date}'
  AND log.dp = 'vova'
  AND log.datasource = 'vova'
  AND log.platform in ('web', 'pc')
) t1
where t1.rank = 1
) t1
left join
(
select
datasource,
domain_userid,
buyer_id,
pt
from
(
select
log.datasource,
log.domain_userid,
log.buyer_id,
log.pt,
row_number() OVER (PARTITION BY log.datasource, log.domain_userid, log.pt ORDER BY dvce_created_tstamp ASC) AS rank
FROM dwd.dwd_vova_log_page_view log
WHERE log.pt = '${cur_date}'
  AND log.dp = 'vova'
  AND log.datasource = 'vova'
  AND log.platform in ('web', 'pc')
  AND log.buyer_id > 0
) t1
where t1.rank = 1
) t2 on t1.datasource = t2.datasource and t1.domain_userid = t2.domain_userid AND t1.pt = t2.pt
left join
(
select
datasource,
domain_userid,
geo_country,
platform,
pt,
original_channel,
dvce_created_ts
from
(
select
datasource,
domain_userid,
geo_country,
platform,
pt,
original_channel,
dvce_created_ts,
row_number() OVER (PARTITION BY datasource, domain_userid, pt ORDER BY dvce_created_ts ASC) AS rank
from
(
select
log.datasource,
log.domain_userid,
log.geo_country,
log.platform,
log.pt,
CASE
    WHEN log.referrer LIKE '%google%'
        THEN 'google'
    WHEN log.referrer LIKE '%facebook%'
        THEN 'facebook'
    WHEN log.referrer LIKE '%youtube%'
        THEN 'youtube'
    WHEN log.page_url LIKE '%Burl_postfix%'
        THEN 'newsletter'
    ELSE 'unknown' END AS original_channel,
log.dvce_created_ts
FROM dwd.dwd_vova_log_page_view log
WHERE log.pt = '${cur_date}'
  AND log.dp = 'vova'
  AND log.datasource = 'vova'
  AND log.platform in ('web', 'pc')
) t1
where t1.original_channel != 'unknown'
) t2
where t2.rank = 1
) t3 on t1.datasource = t3.datasource and t1.domain_userid = t3.domain_userid AND t1.pt = t3.pt
;

INSERT OVERWRITE TABLE dwd.dwd_vova_fact_original_channel
select
/*+ REPARTITION(10) */
f1.datasource,
f1.domain_userid,
f1.region_code,
f1.platform,
if(f2.domain_userid is not null,f2.original_channel, f1.original_channel) AS original_channel,
f1.dvce_created_ts,
nvl(f3.buyer_id, 0) AS buyer_id,
f1.pt as activate_date
from
(
select
*
from
(
select
t1.datasource,
t1.domain_userid,
t1.region_code,
t1.platform,
'unknown' AS original_channel,
t1.dvce_created_ts,
t1.pt,
row_number() OVER (PARTITION BY t1.datasource, t1.domain_userid ORDER BY t1.pt ASC, t1.dvce_created_ts ASC) AS rank
from
dwd.dwd_vova_fact_original_channel_daily t1
) t1
where t1.rank = 1
) f1
left join
(
select
*
from
(
select
t1.datasource,
t1.domain_userid,
t1.region_code,
t1.platform,
t1.original_channel,
t1.dvce_created_ts,
t1.pt,
row_number() OVER (PARTITION BY t1.datasource, t1.domain_userid ORDER BY t1.pt ASC, t1.dvce_created_ts ASC) AS rank
from
dwd.dwd_vova_fact_original_channel_daily t1
where t1.original_channel != 'unknown'
) t1
where t1.rank = 1
) f2 on f1.datasource = f2.datasource and f1.domain_userid = f2.domain_userid
LEFT JOIN
(
SELECT
t3.datasource,
t3.domain_userid,
first(t3.buyer_id) as buyer_id
FROM
dwd.dwd_vova_fact_original_channel_daily t3
WHERE t3.buyer_id > 0
GROUP BY t3.datasource, t3.domain_userid
) f3 on f1.datasource = f3.datasource and f1.domain_userid = f3.domain_userid
;
"

#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=dwd.dwd_vova_fact_web_start_up" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
