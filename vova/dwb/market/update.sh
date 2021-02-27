#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
sql="
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite TABLE dwb.dwb_vova_market PARTITION (pt)
SELECT /*+ REPARTITION(1) */
       rm.event_date,
       rm.datasource,
       rm.region_code,
       nvl(rm.tot_lucky_gmv, 0)       AS tot_lucky_gmv,
       nvl(rm.tot_gmv, 0)             AS tot_gmv,
       nvl(rm.tot_dau, 0)             AS tot_dau,
       nvl(rm.tot_install, 0)         AS tot_install,
       nvl(rm.android_paid_device, 0) AS android_paid_device,
       nvl(rm.android_gmv, 0)         AS android_gmv,
       nvl(rm.android_dau, 0)         AS android_dau,
       nvl(rm.android_install, 0)     AS android_install,
       nvl(rm.ios_paid_device, 0)     AS ios_paid_device,
       nvl(rm.ios_gmv, 0)             AS ios_gmv,
       nvl(rm.ios_dau, 0)             AS ios_dau,
       nvl(rm.ios_install, 0)         AS ios_install,
       nvl(rm.tot_android_1b_ret, 0)  AS tot_android_1b_ret,
       nvl(rm.tot_android_7b_ret, 0)  AS tot_android_7b_ret,
       nvl(rm.tot_android_28b_ret, 0) AS tot_android_28b_ret,
       nvl(rm.tot_ios_1b_ret, 0)      AS tot_ios_1b_ret,
       nvl(rm.tot_ios_7b_ret, 0)      AS tot_ios_7b_ret,
       nvl(rm.tot_ios_28b_ret, 0)     AS tot_ios_28b_ret,
       nvl(rm.new_android_1b_ret, 0)  AS new_android_1b_ret,
       nvl(rm.new_android_7b_ret, 0)  AS new_android_7b_ret,
       nvl(rm.new_android_28b_ret, 0) AS new_android_28b_ret,
       nvl(rm.new_ios_1b_ret, 0)      AS new_ios_1b_ret,
       nvl(rm.new_ios_7b_ret, 0)      AS new_ios_7b_ret,
       nvl(rm.new_ios_28b_ret, 0)     AS new_ios_28b_ret,
       nvl(rm.tot_activate, 0)        AS tot_activate,
       nvl(rm.android_activate, 0)    AS android_activate,
       nvl(rm.ios_activate, 0)        AS ios_activate,
       nvl(tmp1.android_activate, 0)  AS new_android_1b_activate,
       nvl(tmp7.android_activate, 0)  AS new_android_7b_activate,
       nvl(tmp28.android_activate, 0) AS new_android_28b_activate,
       nvl(tmp1.ios_activate, 0)      AS new_ios_1b_activate,
       nvl(tmp7.ios_activate, 0)      AS new_ios_7b_activate,
       nvl(tmp28.ios_activate, 0)     AS new_ios_28b_activate,
       nvl(tmp1.android_dau, 0)       AS tot_android_1b_activate,
       nvl(tmp7.android_dau, 0)       AS tot_android_7b_activate,
       nvl(tmp28.android_dau, 0)      AS tot_android_28b_activate,
       nvl(tmp1.ios_dau, 0)           AS tot_ios_1b_activate,
       nvl(tmp7.ios_dau, 0)           AS tot_ios_7b_activate,
       nvl(tmp28.ios_dau, 0)          AS tot_ios_28b_activate,
       rm.event_date as pt
FROM dwb.dwb_vova_market_process rm
         LEFT JOIN (SELECT date_add(rm1.event_date, 1) AS interval_1,
                           rm1.android_dau,
                           rm1.ios_dau,
                           rm1.android_install,
                           rm1.ios_install,
                           rm1.android_activate,
                           rm1.ios_activate,
                           rm1.datasource,
                           rm1.region_code
                    FROM dwb.dwb_vova_market_process rm1
                    WHERE rm1.pt = '${cur_date}'
) AS tmp1 ON tmp1.interval_1 = rm.event_date
    AND tmp1.datasource = rm.datasource
    AND tmp1.region_code = rm.region_code
         LEFT JOIN (SELECT date_add(rm1.event_date, 7) AS interval_7,
                           rm1.android_dau,
                           rm1.ios_dau,
                           rm1.android_install,
                           rm1.ios_install,
                           rm1.android_activate,
                           rm1.ios_activate,
                           rm1.datasource,
                           rm1.region_code
                    FROM dwb.dwb_vova_market_process rm1
                    WHERE rm1.pt = '${cur_date}'
) AS tmp7 ON tmp7.interval_7 = rm.event_date
    AND tmp7.datasource = rm.datasource
    AND tmp7.region_code = rm.region_code
         LEFT JOIN (SELECT date_add(rm1.event_date, 28) AS interval_28,
                           rm1.android_dau,
                           rm1.ios_dau,
                           rm1.android_install,
                           rm1.ios_install,
                           rm1.android_activate,
                           rm1.ios_activate,
                           rm1.datasource,
                           rm1.region_code
                    FROM dwb.dwb_vova_market_process rm1
                    WHERE rm1.pt = '${cur_date}'
) AS tmp28
                   ON tmp28.interval_28 = rm.event_date
                       AND tmp28.datasource = rm.datasource
                       AND tmp28.region_code = rm.region_code
WHERE rm.pt = '${cur_date}'
AND rm.event_date >= date_sub('${cur_date}', 30)
;
"

#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=dwb_vova_market" \
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
