#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1
#默认日期为一年前的月的第一天
if [ ! -n "$1" ];then
# cur_date=`date -d "-1 day" +%Y-%m-%d`  '${cur_date}'
cur_date=`date +"%Y-%m-01"`
fi

echo "cur_date: $cur_date"
job_name="dwb_vova_user_month_cohort_chenkai_${cur_date}"

###逻辑sql
sql="
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table dwb.dwb_vova_user_month_cohort_arc PARTITION (start_month, end_month)
SELECT /*+ REPARTITION(1) */
  nvl(final.datasource, 'all')    AS datasource,
  nvl(final.region_code, 'all')   AS region_code,
  nvl(final.platform, 'all')      AS platform,
  nvl(final.main_channel, 'all')  AS main_channel,
  nvl(final.is_new, 'all')        AS buyer_type,
  count(DISTINCT final.device_id) AS cohort,
  nvl(final.start_month, 'all')   AS start_month,
  nvl(final.end_month, 'all')     AS end_month
FROM
(
  SELECT
    start_up1.device_id,
    start_up1.start_month,
    start_up2.end_month,
    nvl(start_up1.region_code, 'NALL')  AS region_code,
    nvl(start_up1.platform, 'NA')       AS platform,
    nvl(start_up1.datasource, 'NA')     AS datasource,
    nvl(temp_device.main_channel, 'NA') AS main_channel,
    -- nvl(is_new, 'N') AS is_new
    if(start_up1.start_month = trunc(temp_device.activate_time, 'MM'), 'Y', 'N') is_new
  FROM
  (
    SELECT
      trunc(pt, 'MM') AS start_month,
      su.device_id,
      su.datasource,
      su.region_code,
      su.platform
    FROM dwd.dwd_vova_fact_start_up su
    where trunc(su.pt, 'MM') >= add_months(trunc('${cur_date}', 'MM'), -24) and trunc(su.pt, 'MM') <= '${cur_date}'
    GROUP BY trunc(pt, 'MM'), su.device_id, su.datasource, su.region_code, su.platform
  ) start_up1
  INNER JOIN
  (
    SELECT
      trunc(pt, 'MM') AS end_month,
      su.device_id,
      su.datasource,
      su.region_code,
      su.platform
    FROM dwd.dwd_vova_fact_start_up su
    where trunc(su.pt, 'MM') = '${cur_date}'
    GROUP BY trunc(pt, 'MM'), su.device_id, su.datasource, su.region_code, su.platform
  ) start_up2
  ON start_up1.device_id = start_up2.device_id
  AND start_up1.datasource = start_up2.datasource
  AND start_up1.region_code = start_up2.region_code
  AND start_up1.platform = start_up2.platform
  LEFT JOIN
  (
    SELECT
      dd.device_id,
      nvl(dd.main_channel, 'NA') AS main_channel,
      activate_time,
      dd.datasource
    FROM dim.dim_vova_devices dd
  ) temp_device
  ON temp_device.device_id = start_up1.device_id AND temp_device.datasource = start_up1.datasource
) final
GROUP BY CUBE(final.start_month, final.end_month, final.platform, final.region_code, final.main_channel, final.datasource, final.is_new)
HAVING start_month != 'all' and end_month != 'all'
;

insert overwrite table dwb.dwb_vova_user_month_cohort partition(pt)
select /*+ REPARTITION(1) */
  start_month,
  datasource,
  region_code,
  platform,
  main_channel,
  max(next_0 ),
  max(next_1 ),
  max(next_2 ),
  max(next_3 ),
  max(next_4 ),
  max(next_5 ),
  max(next_6 ),
  max(next_7 ),
  max(next_8 ),
  max(next_9 ),
  max(next_10),
  max(next_11),
  max(next_12),
  max(next_13),
  max(next_14),
  max(next_15),
  max(next_16),
  max(next_17),
  max(next_18),
  max(next_19),
  max(next_20),
  max(next_21),
  max(next_22),
  max(next_23),
  max(next_24),
  buyer_type,
  pt
from
(
  select
    start_month,
    datasource,
    region_code,
    platform,
    main_channel,
    if(months_between(end_month, start_month) = 0 , cohort, NULL) AS next_0,
    if(months_between(end_month, start_month) = 1 , cohort, NULL) AS next_1,
    if(months_between(end_month, start_month) = 2 , cohort, NULL) AS next_2,
    if(months_between(end_month, start_month) = 3 , cohort, NULL) AS next_3,
    if(months_between(end_month, start_month) = 4 , cohort, NULL) AS next_4,
    if(months_between(end_month, start_month) = 5 , cohort, NULL) AS next_5,
    if(months_between(end_month, start_month) = 6 , cohort, NULL) AS next_6,
    if(months_between(end_month, start_month) = 7 , cohort, NULL) AS next_7,
    if(months_between(end_month, start_month) = 8 , cohort, NULL) AS next_8,
    if(months_between(end_month, start_month) = 9 , cohort, NULL) AS next_9,
    if(months_between(end_month, start_month) = 10, cohort, NULL) AS next_10,
    if(months_between(end_month, start_month) = 11, cohort, NULL) AS next_11,
    if(months_between(end_month, start_month) = 12, cohort, NULL) AS next_12,
    if(months_between(end_month, start_month) = 13, cohort, NULL) AS next_13,
    if(months_between(end_month, start_month) = 14, cohort, NULL) AS next_14,
    if(months_between(end_month, start_month) = 15, cohort, NULL) AS next_15,
    if(months_between(end_month, start_month) = 16, cohort, NULL) AS next_16,
    if(months_between(end_month, start_month) = 17, cohort, NULL) AS next_17,
    if(months_between(end_month, start_month) = 18, cohort, NULL) AS next_18,
    if(months_between(end_month, start_month) = 19, cohort, NULL) AS next_19,
    if(months_between(end_month, start_month) = 20, cohort, NULL) AS next_20,
    if(months_between(end_month, start_month) = 21, cohort, NULL) AS next_21,
    if(months_between(end_month, start_month) = 22, cohort, NULL) AS next_22,
    if(months_between(end_month, start_month) = 23, cohort, NULL) AS next_23,
    if(months_between(end_month, start_month) = 24, cohort, NULL) AS next_24,
    buyer_type,
    start_month pt
  from
    dwb.dwb_vova_user_month_cohort_arc
  where start_month >= add_months(trunc('${cur_date}', 'MM'), -24)
) res
group by start_month, datasource, region_code, platform, main_channel, buyer_type,pt
;
"

#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=${job_name}" \
--conf "spark.default.parallelism=500" \
--conf "spark.sql.shuffle.partitions=500" \
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

echo "end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

