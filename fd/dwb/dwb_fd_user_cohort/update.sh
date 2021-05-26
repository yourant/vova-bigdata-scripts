#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
today=`date -d "0 day" +%Y-%m-%d`
date_last1=`date -d $cur_date"+1 day" +%Y-%m-%d`
date_last3=`date -d $cur_date"+3 day" +%Y-%m-%d`
date_last7=`date -d $cur_date"+7 day" +%Y-%m-%d`
date_last15=`date -d $cur_date"+15 day" +%Y-%m-%d`
date_last30=`date -d $cur_date"+30 day" +%Y-%m-%d`

#dependence
#dwd.dwd_fd_session_channel
#ods_fd_snowplow.ods_fd_snowplow_all_event

sql="
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE dwb.dwb_fd_user_cohort PARTITION (pt)
SELECT
/*+ REPARTITION(5) */
base.pt AS event_date,
base.project,
base.country,
base.platform_type,
base.is_new_user,
base.ga_channel,
base.dau,
nvl(cohort.next_1_cnt, 0) AS next_1_cnt,
nvl(cohort.next_3_cnt, 0) AS next_3_cnt,
nvl(cohort.next_7_cnt, 0) AS next_7_cnt,
nvl(cohort.next_15_cnt, 0) AS next_15_cnt,
nvl(cohort.next_30_cnt, 0) AS next_30_cnt,
base.pt
FROM
(
select
count(distinct log.domain_userid) AS dau,
nvl(log.pt, 'all') as pt,
nvl(lower(log.project), 'all') as project,
nvl(
case
when
upper(log.country) in ('FR', 'DE', 'SE', 'GB', 'AU', 'US', 'IT', 'ES', 'NL', 'MX', 'NO', 'AT', 'BE', 'CH', 'DK', 'CZ', 'PL', 'IL', 'BR', 'SA') THEN upper(log.country)
else 'others' end
, 'all') as country,
nvl(nvl(log.platform_type, 'NALL'), 'all') as platform_type,
nvl(
case
when log.session_idx = 1 then 'new'
when log.session_idx > 1 then 'old'
else 'old' end
, 'all') AS is_new_user,
nvl(nvl(lower(dl.ga_channel), 'others'), 'all') AS ga_channel
from ods_fd_snowplow.ods_fd_snowplow_all_event log
left join dwd.dwd_fd_domain_channel dl on dl.domain_userid = log.domain_userid
where log.pt = '${cur_date}'
    and log.project is not null
    and length(log.project) > 2
    and length(log.country) = 2
    and log.event_name not in ('common_impression', 'goods_impression')
group by cube(
log.pt,
lower(log.project),
case
when
upper(log.country) in ('FR', 'DE', 'SE', 'GB', 'AU', 'US', 'IT', 'ES', 'NL', 'MX', 'NO', 'AT', 'BE', 'CH', 'DK', 'CZ', 'PL', 'IL', 'BR', 'SA') THEN upper(log.country)
else 'others' end,
nvl(log.platform_type, 'NALL'),
case
when log.session_idx = 1 then 'new'
when log.session_idx > 1 then 'old'
else 'old' end,
nvl(lower(dl.ga_channel), 'others')
)
) base
LEFT JOIN
(
SELECT count(DISTINCT next_1_cnt)                      AS next_1_cnt,
       count(DISTINCT next_3_cnt)                      AS next_3_cnt,
       count(DISTINCT next_7_cnt)                      AS next_7_cnt,
       count(DISTINCT next_15_cnt)                     AS next_15_cnt,
       count(DISTINCT next_30_cnt)                     AS next_30_cnt,
       nvl(log.pt, 'all')                              AS pt,
       nvl(lower(log.project), 'all')                  AS project,
       nvl(log.country, 'all')                         AS country,
       nvl(nvl(log.platform_type, 'NALL'), 'all')      AS platform_type,
       nvl(log.is_new_user, 'all')                         AS is_new_user,
       nvl(nvl(lower(log.ga_channel), 'others'), 'all') AS ga_channel
FROM (
         SELECT if(datediff(log2.pt, log.pt) = 1, log.domain_userid, NULL)  AS next_1_cnt,
                if(datediff(log2.pt, log.pt) = 3, log.domain_userid, NULL)  AS next_3_cnt,
                if(datediff(log2.pt, log.pt) = 7, log.domain_userid, NULL)  AS next_7_cnt,
                if(datediff(log2.pt, log.pt) = 15, log.domain_userid, NULL) AS next_15_cnt,
                if(datediff(log2.pt, log.pt) = 30, log.domain_userid, NULL) AS next_30_cnt,
                log.pt,
                log.project,
                case when
                  upper(log.country) in ('FR', 'DE', 'SE', 'GB', 'AU', 'US', 'IT', 'ES', 'NL', 'MX', 'NO', 'AT', 'BE', 'CH', 'DK', 'CZ', 'PL', 'IL', 'BR', 'SA') THEN upper(log.country)
                else 'others' end AS country,
                log.platform_type,
                CASE
                    WHEN log.session_idx = 1 THEN 'new'
                    WHEN log.session_idx > 1 THEN 'old'
                    ELSE 'old' END                                          AS is_new_user,
                dl.ga_channel
         FROM ods_fd_snowplow.ods_fd_snowplow_all_event log
                  INNER JOIN
              (
                  SELECT log.project,
                         log.domain_userid,
                         log.pt
                  FROM ods_fd_snowplow.ods_fd_snowplow_all_event log
                  WHERE log.pt IN ('${cur_date}', '${date_last1}', '${date_last3}', '${date_last7}', '${date_last15}', '${date_last30}')
                    AND log.pt != '${today}'
                    AND log.project IS NOT NULL
                    AND length(log.project) > 2
                    AND length(log.country) = 2
                    AND log.event_name NOT IN ('common_impression', 'goods_impression')
                  GROUP BY log.domain_userid, log.pt, log.project
              ) log2 ON log2.domain_userid = log.domain_userid AND log2.project = log.project
                  LEFT JOIN dwd.dwd_fd_domain_channel dl ON dl.domain_userid = log.domain_userid
         WHERE log.pt = '${cur_date}'
           AND log.project IS NOT NULL
           AND length(log.project) > 2
           AND length(log.country) = 2
           AND log.event_name NOT IN ('common_impression', 'goods_impression')
           AND log2.pt > log.pt
           AND datediff(log2.pt, log.pt) IN (1, 3, 7, 15, 30)
     ) log
GROUP BY CUBE ( log.pt,
                lower(log.project),
                log.country,
                nvl(log.platform_type, 'NALL'),
                log.is_new_user,
                nvl(lower(log.ga_channel), 'others')
    )
) cohort on base.pt = cohort.pt
AND base.project = cohort.project
AND base.country = cohort.country
AND base.platform_type = cohort.platform_type
AND base.is_new_user = cohort.is_new_user
AND base.ga_channel = cohort.ga_channel
where base.pt != 'all'
;
"
#echo "$sql"



#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=20" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=dwb_fd_user_cohort" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

