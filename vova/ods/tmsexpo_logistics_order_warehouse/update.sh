#!/bin/bash
#指定日期和引擎
cur_date=$1
his_date=$2
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
his_date=`date -d "1 day ago ${cur_date}" +%Y-%m-%d`
sql="
msck repair table ods_vova_ext.tmsexpo_logistics_order_warehouse_inc;
ALTER TABLE ods_vova_ext.tmsexpo_logistics_order_warehouse_arc DROP IF EXISTS PARTITION (pt = '${cur_date}');
INSERT OVERWRITE table ods_vova_ext.tmsexpo_logistics_order_warehouse_arc PARTITION (pt = '${cur_date}')
SELECT /*+ REPARTITION(40) */ id,
                              logistics_order_sn,
                              tracking_number,
                              in_type,
                              out_time,
                              cmd_out_time,
                              create_time,
                              warehouse_code,
                              last_update_time,
                              in_time,
                              out_type,
                              cmd_in_time,
                              kafka_type
FROM (SELECT id,
             logistics_order_sn,
             tracking_number,
             in_type,
             out_time,
             cmd_out_time,
             create_time,
             warehouse_code,
             last_update_time,
             in_time,
             out_type,
             cmd_in_time,
             kafka_type
      FROM ods_vova_ext.tmsexpo_logistics_order_warehouse_arc
      WHERE pt = '${his_date}') arc
WHERE NOT EXISTS (SELECT 1
                  FROM ods_vova_ext.tmsexpo_logistics_order_warehouse_inc inc
                  WHERE pt = '${cur_date}'
                    and get_json_object(inc.json_str, '$.logistics_order_sn') = arc.logistics_order_sn)
UNION ALL
SELECT id,
       logistics_order_sn,
       tracking_number,
       in_type,
       out_time,
       cmd_out_time,
       create_time,
       warehouse_code,
       last_update_time,
       in_time,
       out_type,
       cmd_in_time,
       kafka_type
FROM (SELECT get_json_object(json_str, '$.id')                 AS                                                                                           id,
             get_json_object(json_str, '$.logistics_order_sn') AS                                                                                           logistics_order_sn,
             get_json_object(json_str, '$.tracking_number')    AS                                                                                           tracking_number,
             get_json_object(json_str, '$.in_type')            AS                                                                                           in_type,
             get_json_object(json_str, '$.out_time')           AS                                                                                           out_time,
             get_json_object(json_str, '$.cmd_out_time')       AS                                                                                           cmd_out_time,
             get_json_object(json_str, '$.create_time')        AS                                                                                           create_time,
             get_json_object(json_str, '$.warehouse_code')     AS                                                                                           warehouse_code,
             get_json_object(json_str, '$.last_update_time')   AS                                                                                           last_update_time,
             get_json_object(json_str, '$.in_time')            AS                                                                                           in_time,
             get_json_object(json_str, '$.out_type')           AS                                                                                           out_type,
             get_json_object(json_str, '$.cmd_in_time')        AS                                                                                           cmd_in_time,
             get_json_object(json_str, '$.kafka_type')         AS                                                                                           kafka_type,
             row_number()
                     over (partition by get_json_object(json_str, '$.logistics_order_sn') order by cast(get_json_object(json_str, '$.id') as bigint) desc ) rk
      FROM ods_vova_ext.tmsexpo_logistics_order_warehouse_inc
      WHERE pt = '${cur_date}') tmp1
where tmp1.rk = 1;

insert overwrite table ods_vova_ext.tmsexpo_logistics_order_warehouse
select /*+ COALESCE(80) */ id,
                           logistics_order_sn,
                           tracking_number,
                           in_type,
                           out_time,
                           cmd_out_time,
                           create_time,
                           warehouse_code,
                           last_update_time,
                           in_time,
                           out_type,
                           cmd_in_time,
                           kafka_type
from ods_vova_ext.tmsexpo_logistics_order_warehouse_arc
where pt = '${cur_date}';
"

spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=30" \
--conf "spark.app.name=ods_tmsexpo_logistics_order_warehouse" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi