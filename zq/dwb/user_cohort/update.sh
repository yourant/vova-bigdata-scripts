#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

sql="
DROP TABLE IF EXISTS tmp.tmp_zq_dwb_cohort_user_cohort_daily;
CREATE TABLE tmp.tmp_zq_dwb_cohort_user_cohort_daily
select
day1.domain_userid,
day1.pt,
datediff(day2.pt, day1.pt) as ddiff
from
(
select
domain_userid,
pt
from
dwd.dwd_zq_fact_web_start_up
where datasource = 'florynight'
AND pt >= date_sub('${cur_date}',30)
group by pt,domain_userid
) day1 INNER JOIN
(
select
domain_userid,
pt
from
dwd.dwd_zq_fact_web_start_up
where datasource = 'florynight'
AND pt >= date_sub('${cur_date}',30)
group by pt,domain_userid
) day2 ON day1.domain_userid = day2.domain_userid
where day2.pt >= day1.pt
AND datediff(day2.pt, day1.pt) in (0, 1, 7, 30)
;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE dwb.dwb_zq_user_cohort_daily PARTITION (pt)
SELECT
/*+ REPARTITION(2) */
nvl(fin.pt, 'all') AS event_date,
'florynight' AS datasource,
nvl(nvl(d1.region_code, 'NALL'), 'all') AS region_code,
nvl(nvl(d1.platform, 'NA'), 'all') AS platform,
nvl(nvl(d1.original_channel, 'unknown'), 'all') AS original_channel,
nvl(if(date(d1.activate_time) = fin.pt, 'Y', 'N'), 'all') AS is_new_active,
count(distinct if(fin.ddiff = 0,fin.domain_userid,null)) AS dau_d0,
count(distinct if(fin.ddiff = 1,fin.domain_userid,null)) AS dau_d1,
count(distinct if(fin.ddiff = 7,fin.domain_userid,null)) AS dau_d7,
count(distinct if(fin.ddiff = 30,fin.domain_userid,null)) AS dau_d30,
nvl(fin.pt, 'all') AS pt
FROM
(
select
*
from
tmp.tmp_zq_dwb_cohort_user_cohort_daily
) fin
inner join dim.dim_zq_domain_userid d1 on d1.domain_userid = fin.domain_userid AND d1.datasource = 'florynight'
group by cube (fin.pt, nvl(d1.region_code, 'NALL'), nvl(d1.platform, 'NA'), nvl(d1.original_channel, 'unknown'), if(date(d1.activate_time) = fin.pt, 'Y', 'N'))
HAVING event_date != 'all'
;


DROP TABLE IF EXISTS tmp.tmp_zq_dwb_cohort_user_cohort_weekly;
CREATE TABLE tmp.tmp_zq_dwb_cohort_user_cohort_weekly
select
day1.domain_userid,
day1.monday_date AS pt,
datediff(day2.monday_date, day1.monday_date) as ddiff
from
(
select
domain_userid,
date_sub(pt, pmod(datediff(pt, '1900-01-08'), 7)) AS monday_date
from
dwd.dwd_zq_fact_web_start_up
where datasource = 'florynight'
AND pt >= date_sub('${cur_date}',105)
group by date_sub(pt, pmod(datediff(pt, '1900-01-08'), 7)), domain_userid
) day1 INNER JOIN
(
select
domain_userid,
date_sub(pt, pmod(datediff(pt, '1900-01-08'), 7)) AS monday_date
from
dwd.dwd_zq_fact_web_start_up
where datasource = 'florynight'
AND pt >= date_sub('${cur_date}',105)
group by date_sub(pt, pmod(datediff(pt, '1900-01-08'), 7)), domain_userid
) day2 ON day1.domain_userid = day2.domain_userid
where day2.monday_date >= day1.monday_date
;

INSERT OVERWRITE TABLE dwb.dwb_zq_user_cohort_weekly PARTITION (pt)
SELECT
/*+ REPARTITION(2) */
nvl(fin.pt, 'all') AS event_date,
'florynight' AS datasource,
nvl(nvl(d1.region_code, 'NALL'), 'all') AS region_code,
nvl(nvl(d1.platform, 'NA'), 'all') AS platform,
nvl(nvl(d1.original_channel, 'unknown'), 'all') AS original_channel,
nvl(if(date_sub(date(d1.activate_time), pmod(datediff(date(d1.activate_time), '1900-01-08'), 7)) = fin.pt, 'Y', 'N'), 'all') AS is_new_active,
count(distinct if(fin.ddiff = 0,fin.domain_userid,null)) AS wau_w0,
count(distinct if(fin.ddiff = 7,fin.domain_userid,null)) AS wau_w1,
count(distinct if(fin.ddiff = 14,fin.domain_userid,null)) AS wau_w2,
count(distinct if(fin.ddiff = 21,fin.domain_userid,null)) AS wau_w3,
count(distinct if(fin.ddiff >0 AND fin.ddiff <= 84,fin.domain_userid,null)) AS wau_quarterly,
nvl(fin.pt, 'all') AS pt
FROM
(
select
*
from
tmp.tmp_zq_dwb_cohort_user_cohort_weekly
) fin
inner join dim.dim_zq_domain_userid d1 on d1.domain_userid = fin.domain_userid AND d1.datasource = 'florynight'
group by cube (fin.pt, nvl(d1.region_code, 'NALL'), nvl(d1.platform, 'NA'), nvl(d1.original_channel, 'unknown'), if(date_sub(date(d1.activate_time), pmod(datediff(date(d1.activate_time), '1900-01-08'), 7)) = fin.pt, 'Y', 'N'))
HAVING event_date != 'all'
;

DROP TABLE IF EXISTS tmp.tmp_zq_dwb_cohort_user_repurchase_daily;
CREATE TABLE tmp.tmp_zq_dwb_cohort_user_repurchase_daily
select
day1.user_id,
day1.pay_date AS pt,
day1.region_code,
day1.platform,
datediff(day2.pay_date, day1.pay_date) as ddiff
from
(
select
oi.user_id,
date(oi.pay_time) AS pay_date,
r.region_code,
if(oi.from_domain like '%api%', 'web', 'pc') AS platform
from
ods_zq_zsp.ods_zq_order_info oi
LEFT JOIN ods_zq_zsp.ods_zq_region r on r.region_id = oi.country
WHERE oi.pay_status >= 1
AND date(oi.pay_time) >= date_sub('${cur_date}', 90)
AND oi.project_name = 'florynight'

group by oi.user_id, date(oi.pay_time), r.region_code, if(oi.from_domain like '%api%', 'web', 'pc')
) day1 INNER JOIN
(
select
oi.user_id,
date(oi.pay_time) AS pay_date
from
ods_zq_zsp.ods_zq_order_info oi
WHERE oi.pay_status >= 1
AND date(oi.pay_time) >= date_sub('${cur_date}', 90)
AND oi.project_name = 'florynight'
group by oi.user_id,date(oi.pay_time)
) day2 ON day1.user_id = day2.user_id
where day2.pay_date >= day1.pay_date
;


INSERT OVERWRITE TABLE dwb.dwb_zq_user_repurchase_daily PARTITION (pt)
SELECT
/*+ REPARTITION(2) */
nvl(fin.pt, 'all') AS event_date,
'florynight' AS datasource,
nvl(nvl(fin.region_code, 'NALL'), 'all') AS region_code,
nvl(nvl(fin.platform, 'NA'), 'all') AS platform,
nvl(nvl(d1.original_channel, 'unknown'), 'all') AS original_channel,
nvl(if(date(d1.activate_time) = fin.pt, 'Y', 'N'), 'all') AS is_new_active,
count(distinct if(fin.ddiff = 0,fin.user_id,null)) AS repurchase_d0,
count(distinct if(fin.ddiff = 1,fin.user_id,null)) AS repurchase_d1,
count(distinct if(fin.ddiff >0 AND fin.ddiff <= 7,fin.user_id,null)) AS repurchase_w1,
count(distinct if(fin.ddiff >0 AND fin.ddiff <= 30,fin.user_id,null)) AS repurchase_m1,
count(distinct if(fin.ddiff >0 AND fin.ddiff <= 90,fin.user_id,null)) AS repurchase_q1,
nvl(fin.pt, 'all') AS pt
FROM
(
select
*
from
tmp.tmp_zq_dwb_cohort_user_repurchase_daily
) fin
left join (
SELECT
first_buyer_id,
domain_userid,
activate_time,
original_channel
FROM
(
select
first_buyer_id,
domain_userid,
activate_time,
original_channel,
row_number() OVER (PARTITION BY first_buyer_id ORDER BY activate_time ASC) AS rank
from
dim.dim_zq_domain_userid
where datasource = 'florynight'
) t1
where t1.rank = 1
) d1 on d1.first_buyer_id = fin.user_id
group by cube (fin.pt, nvl(fin.region_code, 'NALL'), nvl(fin.platform, 'NA'), nvl(d1.original_channel, 'unknown'), if(date(d1.activate_time) = fin.pt, 'Y', 'N'))
HAVING event_date != 'all'
;


DROP TABLE IF EXISTS tmp.tmp_zq_dwb_cohort_user_repurchase_weekly;
CREATE TABLE tmp.tmp_zq_dwb_cohort_user_repurchase_weekly
select
day1.user_id,
day1.monday_date AS pt,
day1.region_code,
day1.platform,
datediff(day2.monday_date, day1.monday_date) as ddiff
from
(
select
oi.user_id,
date_sub(date(oi.pay_time), pmod(datediff(date(oi.pay_time), '1900-01-08'), 7)) AS monday_date,
r.region_code,
if(oi.from_domain like '%api%', 'web', 'pc') AS platform
from
ods_zq_zsp.ods_zq_order_info oi
LEFT JOIN ods_zq_zsp.ods_zq_region r on r.region_id = oi.country
WHERE oi.pay_status >= 1
AND date(oi.pay_time) >= date_sub('${cur_date}', 105)
AND oi.project_name = 'florynight'
group by oi.user_id, date_sub(date(oi.pay_time), pmod(datediff(date(oi.pay_time), '1900-01-08'), 7)), r.region_code, if(oi.from_domain like '%api%', 'web', 'pc')
) day1 INNER JOIN
(
select
oi.user_id,
date_sub(date(oi.pay_time), pmod(datediff(date(oi.pay_time), '1900-01-08'), 7)) AS monday_date
from
ods_zq_zsp.ods_zq_order_info oi
WHERE oi.pay_status >= 1
AND date(oi.pay_time) >= date_sub('${cur_date}', 105)
AND oi.project_name = 'florynight'
group by oi.user_id, date_sub(date(oi.pay_time), pmod(datediff(date(oi.pay_time), '1900-01-08'), 7))
) day2 ON day1.user_id = day2.user_id
where day2.monday_date >= day1.monday_date
;




INSERT OVERWRITE TABLE dwb.dwb_zq_user_repurchase_weekly PARTITION (pt)
SELECT
/*+ REPARTITION(2) */
nvl(fin.pt, 'all') AS event_date,
'florynight' AS datasource,
nvl(nvl(fin.region_code, 'NALL'), 'all') AS region_code,
nvl(nvl(fin.platform, 'NA'), 'all') AS platform,
nvl(nvl(d1.original_channel, 'unknown'), 'all') AS original_channel,
nvl(if(date_sub(date(d1.activate_time), pmod(datediff(date(d1.activate_time), '1900-01-08'), 7)) = fin.pt, 'Y', 'N'), 'all') AS is_new_active,
count(distinct if(fin.ddiff = 0,fin.user_id,null)) AS repurchase_w0,
count(distinct if(fin.ddiff = 7,fin.user_id,null)) AS repurchase_w1,
count(distinct if(fin.ddiff = 14,fin.user_id,null)) AS repurchase_w2,
count(distinct if(fin.ddiff = 21,fin.user_id,null)) AS repurchase_w3,
count(distinct if(fin.ddiff >0 AND fin.ddiff <= 84,fin.user_id,null)) AS repurchase_quarterly,
nvl(fin.pt, 'all') AS pt
FROM
(
select
*
from
tmp.tmp_zq_dwb_cohort_user_repurchase_weekly
) fin
left join (
SELECT
first_buyer_id,
domain_userid,
activate_time,
original_channel
FROM
(
select
first_buyer_id,
domain_userid,
activate_time,
original_channel,
row_number() OVER (PARTITION BY first_buyer_id ORDER BY activate_time ASC) AS rank
from
dim.dim_zq_domain_userid
where datasource = 'florynight'
) t1
where t1.rank = 1
) d1 on d1.first_buyer_id = fin.user_id
group by cube (fin.pt, nvl(fin.region_code, 'NALL'), nvl(fin.platform, 'NA'), nvl(d1.original_channel, 'unknown'), if(date_sub(date(d1.activate_time), pmod(datediff(date(d1.activate_time), '1900-01-08'), 7)) = fin.pt, 'Y', 'N'))
HAVING event_date != 'all'
;
"

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=zq_fn_user_cohort" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi


