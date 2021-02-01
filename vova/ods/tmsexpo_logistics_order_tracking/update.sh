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
msck repair table ods_vova_ext.tmsexpo_logistics_order_tracking_inc;
INSERT OVERWRITE table ods_vova_ext.tmsexpo_logistics_order_tracking_arc PARTITION (pt = '${cur_date}')
SELECT /*+ REPARTITION(40) */ id,
       tracking_number,
       carrier_id,
       logistics_order_sn,
       depart_airport_failed_time,
       depart_first_mile_time,
       delivery_failed_time,
       warehouse_code,
       arrive_dest_airport_time,
       tracking_status,
       transfer_last_mile_time,
       depart_dest_customs_time,
       pick_up_failed_time,
       origin_country_code,
       active_node_time,
       first_obtain_time,
       pick_up_time,
       estimated_delivery_date,
       linehaul_pick_up_failed_time,
       epc_sign_time,
       destination_country_code,
       depart_epc_wh_time,
       shipping_time,
       arrive_epc_wh_time,
       create_time,
       linehaul_pick_up_time,
       arrive_last_mile_time,
       tracking_tag,
       last_update_time,
       dest_customs_failure_time,
       delivered_time,
       arrive_first_mile_time,
       platform_id,
       data_type,
       depart_airport_time,
       shipping_label_time,
       attempt_deliver_time,
       kafka_type
FROM (SELECT id,
             tracking_number,
             carrier_id,
             logistics_order_sn,
             depart_airport_failed_time,
             depart_first_mile_time,
             delivery_failed_time,
             warehouse_code,
             arrive_dest_airport_time,
             tracking_status,
             transfer_last_mile_time,
             depart_dest_customs_time,
             pick_up_failed_time,
             origin_country_code,
             active_node_time,
             first_obtain_time,
             pick_up_time,
             estimated_delivery_date,
             linehaul_pick_up_failed_time,
             epc_sign_time,
             destination_country_code,
             depart_epc_wh_time,
             shipping_time,
             arrive_epc_wh_time,
             create_time,
             linehaul_pick_up_time,
             arrive_last_mile_time,
             tracking_tag,
             last_update_time,
             dest_customs_failure_time,
             delivered_time,
             arrive_first_mile_time,
             platform_id,
             data_type,
             depart_airport_time,
             shipping_label_time,
             attempt_deliver_time,
             kafka_type
      FROM ods_vova_ext.tmsexpo_logistics_order_tracking_arc
      WHERE pt = '${his_date}') arc
WHERE NOT EXISTS (SELECT 1
                  FROM ods_vova_ext.tmsexpo_logistics_order_tracking_inc inc
                  WHERE pt = '${cur_date}'
                    and get_json_object(inc.json_str, '$.logistics_order_sn') = arc.logistics_order_sn)
UNION ALL
SELECT id,
       tracking_number,
       carrier_id,
       logistics_order_sn,
       depart_airport_failed_time,
       depart_first_mile_time,
       delivery_failed_time,
       warehouse_code,
       arrive_dest_airport_time,
       tracking_status,
       transfer_last_mile_time,
       depart_dest_customs_time,
       pick_up_failed_time,
       origin_country_code,
       active_node_time,
       first_obtain_time,
       pick_up_time,
       estimated_delivery_date,
       linehaul_pick_up_failed_time,
       epc_sign_time,
       destination_country_code,
       depart_epc_wh_time,
       shipping_time,
       arrive_epc_wh_time,
       create_time,
       linehaul_pick_up_time,
       arrive_last_mile_time,
       tracking_tag,
       last_update_time,
       dest_customs_failure_time,
       delivered_time,
       arrive_first_mile_time,
       platform_id,
       data_type,
       depart_airport_time,
       shipping_label_time,
       attempt_deliver_time,
       kafka_type
FROM (SELECT get_json_object(json_str, '$.id')                           AS                                                                                 id,
             get_json_object(json_str, '$.tracking_number')              AS                                                                                 tracking_number,
             get_json_object(json_str, '$.carrier_id')                   AS                                                                                 carrier_id,
             get_json_object(json_str, '$.logistics_order_sn')           AS                                                                                 logistics_order_sn,
             get_json_object(json_str, '$.depart_airport_failed_time')   AS                                                                                 depart_airport_failed_time,
             get_json_object(json_str, '$.depart_first_mile_time')       AS                                                                                 depart_first_mile_time,
             get_json_object(json_str, '$.delivery_failed_time')         AS                                                                                 delivery_failed_time,
             get_json_object(json_str, '$.warehouse_code')               AS                                                                                 warehouse_code,
             get_json_object(json_str, '$.arrive_dest_airport_time')     AS                                                                                 arrive_dest_airport_time,
             get_json_object(json_str, '$.tracking_status')              AS                                                                                 tracking_status,
             get_json_object(json_str, '$.transfer_last_mile_time')      AS                                                                                 transfer_last_mile_time,
             get_json_object(json_str, '$.depart_dest_customs_time')     AS                                                                                 depart_dest_customs_time,
             get_json_object(json_str, '$.pick_up_failed_time')          AS                                                                                 pick_up_failed_time,
             get_json_object(json_str, '$.origin_country_code')          AS                                                                                 origin_country_code,
             get_json_object(json_str, '$.active_node_time')             AS                                                                                 active_node_time,
             get_json_object(json_str, '$.first_obtain_time')            AS                                                                                 first_obtain_time,
             get_json_object(json_str, '$.pick_up_time')                 AS                                                                                 pick_up_time,
             get_json_object(json_str, '$.estimated_delivery_date')      AS                                                                                 estimated_delivery_date,
             get_json_object(json_str, '$.linehaul_pick_up_failed_time') AS                                                                                 linehaul_pick_up_failed_time,
             get_json_object(json_str, '$.epc_sign_time')                AS                                                                                 epc_sign_time,
             get_json_object(json_str, '$.destination_country_code')     AS                                                                                 destination_country_code,
             get_json_object(json_str, '$.depart_epc_wh_time')           AS                                                                                 depart_epc_wh_time,
             get_json_object(json_str, '$.shipping_time')                AS                                                                                 shipping_time,
             get_json_object(json_str, '$.arrive_epc_wh_time')           AS                                                                                 arrive_epc_wh_time,
             get_json_object(json_str, '$.create_time')                  AS                                                                                 create_time,
             get_json_object(json_str, '$.linehaul_pick_up_time')        AS                                                                                 linehaul_pick_up_time,
             get_json_object(json_str, '$.arrive_last_mile_time')        AS                                                                                 arrive_last_mile_time,
             get_json_object(json_str, '$.tracking_tag')                 AS                                                                                 tracking_tag,
             get_json_object(json_str, '$.last_update_time')             AS                                                                                 last_update_time,
             get_json_object(json_str, '$.dest_customs_failure_time')    AS                                                                                 dest_customs_failure_time,
             get_json_object(json_str, '$.delivered_time')               AS                                                                                 delivered_time,
             get_json_object(json_str, '$.arrive_first_mile_time')       AS                                                                                 arrive_first_mile_time,
             get_json_object(json_str, '$.platform_id')                  AS                                                                                 platform_id,
             get_json_object(json_str, '$.data_type')                    AS                                                                                 data_type,
             get_json_object(json_str, '$.depart_airport_time')          AS                                                                                 depart_airport_time,
             get_json_object(json_str, '$.shipping_label_time')          AS                                                                                 shipping_label_time,
             get_json_object(json_str, '$.attempt_deliver_time')         AS                                                                                 attempt_deliver_time,
             get_json_object(json_str, '$.kafka_type')                   AS                                                                                 kafka_type,
             row_number()
                     over (partition by get_json_object(json_str, '$.logistics_order_sn'),get_json_object(json_str, '$.data_type') order by cast(get_json_object(json_str, '$.id') as bigint) desc ) rk
      FROM ods_vova_ext.tmsexpo_logistics_order_tracking_inc
      WHERE pt = '${cur_date}') tmp1
where tmp1.rk = 1;


insert overwrite table ods_vova_ext.tmsexpo_logistics_order_tracking
select /*+ COALESCE(80) */ id,
       tracking_number,
       carrier_id,
       logistics_order_sn,
       depart_airport_failed_time,
       depart_first_mile_time,
       delivery_failed_time,
       warehouse_code,
       arrive_dest_airport_time,
       tracking_status,
       transfer_last_mile_time,
       depart_dest_customs_time,
       pick_up_failed_time,
       origin_country_code,
       active_node_time,
       first_obtain_time,
       pick_up_time,
       estimated_delivery_date,
       linehaul_pick_up_failed_time,
       epc_sign_time,
       destination_country_code,
       depart_epc_wh_time,
       shipping_time,
       arrive_epc_wh_time,
       create_time,
       linehaul_pick_up_time,
       arrive_last_mile_time,
       tracking_tag,
       last_update_time,
       dest_customs_failure_time,
       delivered_time,
       arrive_first_mile_time,
       platform_id,
       data_type,
       depart_airport_time,
       shipping_label_time,
       attempt_deliver_time,
       kafka_type
from ods_vova_ext.tmsexpo_logistics_order_tracking_arc
where pt = '${cur_date}';
"

spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=30" \
--conf "spark.app.name=ods_tmsexpo_logistics_order_tracking" \
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