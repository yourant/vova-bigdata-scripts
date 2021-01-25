#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
sql="
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite TABLE dwb.dwb_vova_order_goods_life_cycle_cnt PARTITION ( pt )
SELECT
nvl ( nvl ( og.region_code, 'NALL' ), 'ALL' ) AS ctry,
count( * ) AS order_cnt,
count( fp.order_goods_id ) AS pay_cnt,
sum( IF ( og.sku_order_status = 0 AND ogs.sku_pay_status IN ( 2, 11 ), 1, 0 ) ) AS unconfirmed_cnt,
sum( IF ( og.sku_order_status = 2 AND ogs.sku_pay_status > 0, 1, 0 ) ) AS cancel_cnt,
sum( IF ( ge.storage_type = 3 AND fp.order_goods_id IS NOT NULL AND og.sku_order_status != 2, 1, 0 ) ) AS pre_warehouse_cnt,
sum( IF(cog.combine_type IN ( 2, 3 ) AND fp.order_goods_id IS NOT NULL AND og.sku_order_status != 2, 1, 0 ) ) AS jiewang_cnt,
sum( IF ( cog.combine_type = 1 AND fp.order_goods_id IS NOT NULL AND og.sku_order_status != 2, 1, 0 ) ) AS yanwen_cnt,
sum( IF ( ge.collection_plan_id = 0 AND fp.order_goods_id IS NOT NULL AND og.sku_order_status != 2, 1, 0 ) ) AS common_cnt,
nvl ( to_date ( og.order_time ), 'ALL' ) AS pt
FROM
  dim.dim_vova_order_goods og
  LEFT JOIN dwd.dwd_vova_fact_pay fp ON og.order_goods_id = fp.order_goods_id
  LEFT JOIN ods_vova_vts.ods_vova_order_goods_extra ge ON og.order_goods_id = ge.order_goods_id
  LEFT JOIN ods_vova_vts.ods_vova_collection_order_goods cog ON cog.order_goods_id = og.order_goods_id
  left join ods_vova_vts.ods_vova_order_goods_status ogs on og.order_goods_id = ogs.order_goods_id
WHERE
  to_date ( og.order_time ) >= date_sub('${cur_date}',30) and to_date ( og.order_time ) <= '${cur_date}'
GROUP BY
  to_date ( og.order_time ),
  nvl ( og.region_code, 'NALL' ) WITH cube
HAVING
  pt != 'ALL'
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=dwb_vova_order_goods_life_cycle_cnt" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 280" \
--conf "spark.sql.shuffle.partitions=280" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi