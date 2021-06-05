#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

month=$1
#指定日期和引擎
cur_date=$2

if [ ! -n "$1" ]; then
  month=24
fi
#默认日期为一年前的月的第一天
if [ ! -n "$2" ];then
# cur_date=`date -d "-1 day" +%Y-%m-%d`  '${cur_date}'
cur_date=`date +"%Y-%m-01" -d "-${month} month"`
fi

echo "cur_date: $cur_date"
job_name="dwb_vova_user_month_cohort_req_chenkai_${cur_date}"

###逻辑sql
sql="
insert overwrite table tmp.tmp_vova_user_month_cohort_v2 PARTITION (pt='${cur_date}')
SELECT /*+ REPARTITION(1) */
  nvl(final.start_month, 'all')  AS start_month,
  nvl(final.datasource, 'all')  AS datasource,
  nvl(final.region_code, 'all')  AS region_code,
  nvl(final.platform, 'all')     AS platform,
  nvl(final.main_channel, 'all') AS main_channel,
  count(DISTINCT final.next_0)   AS next_0_num,
  count(DISTINCT final.next_1)   AS next_1_num,
  count(DISTINCT final.next_2)   AS next_2_num,
  count(DISTINCT final.next_3)   AS next_3_num,
  count(DISTINCT final.next_4)   AS next_4_num,
  count(DISTINCT final.next_5)   AS next_5_num,
  count(DISTINCT final.next_6)   AS next_6_num,
  count(DISTINCT final.next_7)   AS next_7_num,
  count(DISTINCT final.next_8)   AS next_8_num,
  count(DISTINCT final.next_9)   AS next_9_num,
  count(DISTINCT final.next_10)  AS next_10_num,
  count(DISTINCT final.next_11)  AS next_11_num,
  count(DISTINCT final.next_12)  AS next_12_num,
  count(DISTINCT final.next_13)  AS next_13_num,
  count(DISTINCT final.next_14)  AS next_14_num,
  count(DISTINCT final.next_15)  AS next_15_num,
  count(DISTINCT final.next_16)  AS next_16_num,
  count(DISTINCT final.next_17)  AS next_17_num,
  count(DISTINCT final.next_18)  AS next_18_num,
  count(DISTINCT final.next_19)  AS next_19_num,
  count(DISTINCT final.next_20)  AS next_20_num,
  count(DISTINCT final.next_21)  AS next_21_num,
  count(DISTINCT final.next_22)  AS next_22_num,
  count(DISTINCT final.next_23)  AS next_23_num,
  count(DISTINCT final.next_24)  AS next_24_num,
  nvl(final.is_new, 'all') AS buyer_type
FROM
(
  SELECT
    if(months_between(start_up2.start_month, start_up1.start_month) = 0, start_up1.device_id, NULL)  AS next_0,
    if(months_between(start_up2.start_month, start_up1.start_month) = 1, start_up1.device_id, NULL)  AS next_1,
    if(months_between(start_up2.start_month, start_up1.start_month) = 2, start_up1.device_id, NULL)  AS next_2,
    if(months_between(start_up2.start_month, start_up1.start_month) = 3, start_up1.device_id, NULL)  AS next_3,
    if(months_between(start_up2.start_month, start_up1.start_month) = 4, start_up1.device_id, NULL)  AS next_4,
    if(months_between(start_up2.start_month, start_up1.start_month) = 5, start_up1.device_id, NULL)  AS next_5,
    if(months_between(start_up2.start_month, start_up1.start_month) = 6, start_up1.device_id, NULL)  AS next_6,
    if(months_between(start_up2.start_month, start_up1.start_month) = 7, start_up1.device_id, NULL)  AS next_7,
    if(months_between(start_up2.start_month, start_up1.start_month) = 8, start_up1.device_id, NULL)  AS next_8,
    if(months_between(start_up2.start_month, start_up1.start_month) = 9, start_up1.device_id, NULL)  AS next_9,
    if(months_between(start_up2.start_month, start_up1.start_month) = 10, start_up1.device_id, NULL) AS next_10,
    if(months_between(start_up2.start_month, start_up1.start_month) = 11, start_up1.device_id, NULL) AS next_11,
    if(months_between(start_up2.start_month, start_up1.start_month) = 12, start_up1.device_id, NULL) AS next_12,
    if(months_between(start_up2.start_month, start_up1.start_month) = 13, start_up1.device_id, NULL) AS next_13,
    if(months_between(start_up2.start_month, start_up1.start_month) = 14, start_up1.device_id, NULL) AS next_14,
    if(months_between(start_up2.start_month, start_up1.start_month) = 15, start_up1.device_id, NULL) AS next_15,
    if(months_between(start_up2.start_month, start_up1.start_month) = 16, start_up1.device_id, NULL) AS next_16,
    if(months_between(start_up2.start_month, start_up1.start_month) = 17, start_up1.device_id, NULL) AS next_17,
    if(months_between(start_up2.start_month, start_up1.start_month) = 18, start_up1.device_id, NULL) AS next_18,
    if(months_between(start_up2.start_month, start_up1.start_month) = 19, start_up1.device_id, NULL) AS next_19,
    if(months_between(start_up2.start_month, start_up1.start_month) = 20, start_up1.device_id, NULL) AS next_20,
    if(months_between(start_up2.start_month, start_up1.start_month) = 21, start_up1.device_id, NULL) AS next_21,
    if(months_between(start_up2.start_month, start_up1.start_month) = 22, start_up1.device_id, NULL) AS next_22,
    if(months_between(start_up2.start_month, start_up1.start_month) = 23, start_up1.device_id, NULL) AS next_23,
    if(months_between(start_up2.start_month, start_up1.start_month) = 24, start_up1.device_id, NULL) AS next_24,
    start_up1.start_month,
    nvl(start_up1.region_code, 'NALL')  AS region_code,
    nvl(start_up1.platform, 'NA')     AS platform,
    nvl(start_up1.datasource, 'NA') AS datasource,
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
    where trunc(su.pt, 'MM') = '${cur_date}'
    GROUP BY trunc(pt, 'MM'), su.device_id, su.datasource, su.region_code, su.platform
  ) start_up1
  INNER JOIN
  (

    SELECT
      start_month,
      t1.device_id,
      t1.datasource,
      t1.region_code,
      t1.platform
    from
    (
      select
        trunc(pt, 'MM') AS start_month,
        device_id,
        datasource,
        region_code,
        platform
      from
        dwd.dwd_vova_fact_start_up_m
      where month >= '${cur_date}'
      union all
      select
        trunc(pt, 'MM') AS start_month,
        su.device_id,
        su.datasource,
        su.region_code,
        su.platform
      FROM dwd.dwd_vova_fact_start_up su
      where pt >= add_months(trunc('${cur_date}', 'MM'), ${month})
    ) t1
    GROUP BY t1.start_month, t1.device_id, t1.datasource, t1.region_code, t1.platform

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
      -- if(trunc('${cur_date}', 'MM') = trunc(dd.activate_time, 'MM'), 'Y', 'N') is_new,
      dd.datasource
    FROM dim.dim_vova_devices dd
  ) temp_device
  ON temp_device.device_id = start_up1.device_id AND temp_device.datasource = start_up1.datasource
) final
GROUP BY CUBE(final.start_month, final.platform, final.region_code, final.main_channel, final.datasource, final.is_new)
HAVING start_month != 'all'
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

echo "end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

