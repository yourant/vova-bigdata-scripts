#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1
#默认日期为一年前的月的第一天
if [ ! -n "$1" ];then
# cur_date=`date -d "-1 day" +%Y-%m-%d`  '${cur_date}'
cur_date=`date +"%Y-%m-01" -d "-13 month"`
fi

echo "cur_date: $cur_date"
job_name="dwb_vova_new_user_month_cohort_req_chenkai_${cur_date}"

###逻辑sql

#首次支付用户月度留存
sql="

drop table if exists tmp.tmp_order_goods_cnt;
create table tmp.tmp_order_goods_cnt as
select
/*+ REPARTITION(2) */
    device_id,
    datasource,
    -- region_code,
    -- platform,
    start_month,
    nvl(sum(pre_direct_cnt), 0) pre_direct_cnt,
    count(*) order_cnt
from
    (select order_goods_id,
            device_id,
            datasource,
            -- region_code,
            -- platform,
            trunc(pay_time,'MM') start_month
     from dwd.dwd_vova_fact_pay
     where pay_time >= '${cur_date}') tmp_fp
        left JOIN
    (select order_goods_id,
            case when combine_type=2 then 1
                 else 0 end pre_direct_cnt
     from ods_vova_vts.ods_vova_collection_order_goods) vcog
    on tmp_fp.order_goods_id = vcog.order_goods_id
group by device_id,datasource,
         -- region_code,platform,
         start_month
;


set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwb.dwb_vova_order_month_start_up_cohort PARTITION (pt)
SELECT /*+ REPARTITION(1) */
  nvl(final.first_pay_month, 'all') AS pay_month,
  nvl(final.datasource, 'all')     AS datasource,
  nvl(final.region_code, 'all')     AS region_code,
  nvl(final.platform, 'all')        AS platform,
  count(DISTINCT final.next_0)      AS next_0_num,
  count(DISTINCT final.next_1)      AS next_1_num,
  count(DISTINCT final.next_2)      AS next_2_num,
  count(DISTINCT final.next_3)      AS next_3_num,
  count(DISTINCT final.next_4)      AS next_4_num,
  count(DISTINCT final.next_5)      AS next_5_num,
  count(DISTINCT final.next_6)      AS next_6_num,
  count(DISTINCT final.next_7)      AS next_7_num,
  count(DISTINCT final.next_8)      AS next_8_num,
  count(DISTINCT final.next_9)      AS next_9_num,
  count(DISTINCT final.next_10)     AS next_10_num,
  count(DISTINCT final.next_11)     AS next_11_num,
  count(DISTINCT final.next_12)     AS next_12_num,
  nvl(final.is_new, 'all')      AS buyer_type, -- 用户是否当月新激活
  nvl(final.first_pay_month, 'all') AS pt
FROM (
  SELECT temp_devices.device_id AS next_0,
    if(months_between(temp2.start_month, temp_devices.first_pay_month) = 1, temp_devices.device_id, NULL) AS next_1,
    if(months_between(temp2.start_month, temp_devices.first_pay_month) = 2, temp_devices.device_id, NULL) AS next_2,
    if(months_between(temp2.start_month, temp_devices.first_pay_month) = 3, temp_devices.device_id, NULL) AS next_3,
    if(months_between(temp2.start_month, temp_devices.first_pay_month) = 4, temp_devices.device_id, NULL) AS next_4,
    if(months_between(temp2.start_month, temp_devices.first_pay_month) = 5, temp_devices.device_id, NULL) AS next_5,
    if(months_between(temp2.start_month, temp_devices.first_pay_month) = 6, temp_devices.device_id, NULL) AS next_6,
    if(months_between(temp2.start_month, temp_devices.first_pay_month) = 7, temp_devices.device_id, NULL) AS next_7,
    if(months_between(temp2.start_month, temp_devices.first_pay_month) = 8, temp_devices.device_id, NULL) AS next_8,
    if(months_between(temp2.start_month, temp_devices.first_pay_month) = 9, temp_devices.device_id, NULL) AS next_9,
    if(months_between(temp2.start_month, temp_devices.first_pay_month) = 10, temp_devices.device_id, NULL) AS next_10,
    if(months_between(temp2.start_month, temp_devices.first_pay_month) = 11, temp_devices.device_id, NULL) AS next_11,
    if(months_between(temp2.start_month, temp_devices.first_pay_month) = 12, temp_devices.device_id, NULL) AS next_12,
    temp_devices.first_pay_month,
    temp_devices.region_code,
    temp_devices.platform,
    temp_devices.datasource,
    temp_devices.is_new AS is_new
  FROM
  (
    SELECT
      trunc(tdfp.first_pay_time, 'MM') AS first_pay_month,
      tdfp.device_id,
      nvl(dd.platform, 'NA') AS platform,
      nvl(dd.region_code, 'NALL') AS region_code,
      nvl(tdfp.datasource, 'NA') AS datasource,
      if(trunc(tdfp.first_pay_time, 'MM') = trunc(activate_time, 'MM'), 'Y', 'N') is_new
    FROM
    (
      select *
      from
        tmp.tmp_vova_device_first_pay
      where first_pay_time >= '${cur_date}'
    ) tdfp
    LEFT JOIN
      dim.dim_vova_devices dd
    ON dd.device_id = tdfp.device_id AND tdfp.datasource = dd.datasource
    WHERE tdfp.first_pay_time IS NOT NULL
      AND tdfp.device_id IS NOT NULL
    GROUP BY trunc(tdfp.first_pay_time, 'MM'), tdfp.device_id, nvl(dd.platform, 'NA'), is_new, nvl(dd.region_code, 'NALL'), nvl(tdfp.datasource, 'NA')
  ) temp_devices
  LEFT JOIN
  (
    SELECT trunc(pt, 'MM') AS start_month,
      su.device_id,
      su.datasource
    FROM dwd.dwd_vova_fact_start_up su
    GROUP BY trunc(pt, 'MM'), su.device_id, su.datasource
  ) temp2
  ON temp_devices.device_id = temp2.device_id
    AND temp2.datasource = temp_devices.datasource
    AND temp2.start_month >= temp_devices.first_pay_month
    AND months_between(temp2.start_month, temp_devices.first_pay_month) <= 12
) final
GROUP BY CUBE(final.first_pay_month, final.platform, final.region_code, final.datasource, final.is_new)
HAVING pay_month != 'all';
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

echo "order_month_start_up_cohort is ok, end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

job_name="dwb_vova_user_month_repurchase_cohort_req_chenkai_${cur_date}"

#用户复购月度留存
sql="
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
-- insert overwrite table dwb.dwb_vova_order_month_cohort PARTITION (pt)
insert overwrite table dwb.dwb_vova_order_month_cohort PARTITION (pt)
SELECT /*+ REPARTITION(1) */
  nvl(final.pay_month, 'all')   AS pay_month,
  count(DISTINCT final.next_0)  AS next_0_num,
  count(DISTINCT final.next_1)  AS next_1_num,
  count(DISTINCT final.next_2)  AS next_2_num,
  count(DISTINCT final.next_3)  AS next_3_num,
  count(DISTINCT final.next_4)  AS next_4_num,
  count(DISTINCT final.next_5)  AS next_5_num,
  count(DISTINCT final.next_6)  AS next_6_num,
  count(DISTINCT final.next_7)  AS next_7_num,
  count(DISTINCT final.next_8)  AS next_8_num,
  count(DISTINCT final.next_9)  AS next_9_num,
  count(DISTINCT final.next_10) AS next_10_num,
  count(DISTINCT final.next_11) AS next_11_num,
  count(DISTINCT final.next_12) AS next_12_num,
  nvl(final.is_new_user, 'all') AS is_new_user,
  nvl(final.region_code, 'all') AS region_code,
  nvl(final.platform, 'all')    AS platform,
  nvl(final.datasource, 'all')  AS datasource,
  nvl(final.is_new, 'all')      AS buyer_type, -- 替换为: 用户是否当月新激活
  nvl(final.pay_month, 'all')   AS pt
FROM
(
  SELECT
    if(months_between(temp2.pay_month, temp1.pay_month) = 0, temp1.device_id, NULL) AS next_0,
    if(months_between(temp2.pay_month, temp1.pay_month) = 1, temp1.device_id, NULL) AS next_1,
    if(months_between(temp2.pay_month, temp1.pay_month) = 2, temp1.device_id, NULL) AS next_2,
    if(months_between(temp2.pay_month, temp1.pay_month) = 3, temp1.device_id, NULL) AS next_3,
    if(months_between(temp2.pay_month, temp1.pay_month) = 4, temp1.device_id, NULL) AS next_4,
    if(months_between(temp2.pay_month, temp1.pay_month) = 5, temp1.device_id, NULL) AS next_5,
    if(months_between(temp2.pay_month, temp1.pay_month) = 6, temp1.device_id, NULL) AS next_6,
    if(months_between(temp2.pay_month, temp1.pay_month) = 7, temp1.device_id, NULL) AS next_7,
    if(months_between(temp2.pay_month, temp1.pay_month) = 8, temp1.device_id, NULL) AS next_8,
    if(months_between(temp2.pay_month, temp1.pay_month) = 9, temp1.device_id, NULL) AS next_9,
    if(months_between(temp2.pay_month, temp1.pay_month) = 10, temp1.device_id, NULL) AS next_10,
    if(months_between(temp2.pay_month, temp1.pay_month) = 11, temp1.device_id, NULL) AS next_11,
    if(months_between(temp2.pay_month, temp1.pay_month) = 12, temp1.device_id, NULL) AS next_12,
    temp1.pay_month,
    temp1.region_code,
    temp1.platform,
    temp1.datasource,
    nvl(if(temp_first_pay.device_id IS NOT NULL, 'Y', 'N'), 'N') AS is_new_user,
    -- nvl(tmp_pre_direct.buyer_type, 'no_pre_direct') AS buyer_type
    if(trunc(temp1.pay_month, 'MM') = trunc(dd.activate_time, 'MM'), 'Y', 'N') is_new
  FROM
  (
    SELECT
      trunc(pay_time, 'MM') AS pay_month,
      fp.device_id,
      fp.region_code,
      fp.platform,
      fp.datasource
    FROM dwd.dwd_vova_fact_pay fp where fp.pay_time > '${cur_date}'
    GROUP BY trunc(pay_time, 'MM'), fp.device_id, fp.region_code, fp.platform, fp.datasource
  ) temp1
  left join
    dim.dim_vova_devices dd
  ON dd.device_id = temp1.device_id AND temp1.datasource = dd.datasource
  LEFT JOIN
  (
    SELECT
      trunc(first_pay_time, 'MM') AS first_pay_month,
      tdfp.device_id,
      tdfp.datasource
    FROM tmp.tmp_vova_device_first_pay tdfp
    WHERE first_pay_time IS NOT NULL
    GROUP BY trunc(first_pay_time, 'MM'), tdfp.device_id, tdfp.datasource
  ) temp_first_pay
  ON temp1.device_id = temp_first_pay.device_id
    AND temp_first_pay.datasource = temp1.datasource
    AND temp_first_pay.first_pay_month = temp1.pay_month
  INNER JOIN
  (
    SELECT trunc(pay_time, 'MM') AS pay_month,
      fp2.device_id,
      fp2.region_code,
      fp2.platform,
      fp2.datasource
    FROM dwd.dwd_vova_fact_pay fp2
    GROUP BY trunc(pay_time, 'MM'), fp2.device_id, fp2.region_code, fp2.platform, fp2.datasource
  ) temp2
  ON temp1.device_id = temp2.device_id
    AND temp2.region_code = temp1.region_code
    AND temp2.platform = temp1.platform
    AND temp2.datasource = temp1.datasource
    AND temp2.pay_month >= temp1.pay_month
    AND months_between(temp2.pay_month, temp1.pay_month) <= 12
) final
GROUP BY CUBE (final.pay_month, final.is_new_user, final.platform, final.region_code, final.datasource, final.is_new)
HAVING pay_month != 'all'
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

