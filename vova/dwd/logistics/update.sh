#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
hadoop fs -mkdir s3://bigdata-offline/warehouse/dwd/dwd_vova_fact_logistics
###逻辑sql
#oge.collection_plan_id IN (1, 2)是集运
sql="
insert overwrite table dwd.dwd_vova_fact_logistics
select /*+ REPARTITION(100) */
       case
           when oi.from_domain like '%vova%' then 'vova'
           when oi.from_domain like '%airyclub%' then 'airyclub'
           end as datasource,
       tracking_detail_id,
       ost.tracking_id,
       ost.order_goods_id,
       oge.combine_id,
       oge.collection_plan_id,
       ost.shipping_carrier_id,
       ost.shipping_carrier,
       ost.create_time,
       ost.first_update_date,
       ost.valid_tracking_date,
       ost.process_tag,
       ost.shipment_type,
       ost.error_code,
       ost.tracking_status,
       ost.tracking_time,
       ost.first_tracking_time,
       ost.source_id,
       ost.origin_country,
       ost.destination_country,
       ostd.service_type,
       ostd.weight,
       ostd.recipient_name,
       ostd.pickup_status,
       ostd.is_in_collection,
       ostd.in_collection_weight,
       ostd.in_collection_time,
       ostd.is_out_collection,
       ostd.out_collection_weight,
       ostd.out_collection_time,
       ostd.is_delivered,
       if(date_format(ost.delivered_date, 'yyyy-MM-dd HH:mm:ss') is not null,date_format(ost.delivered_date, 'yyyy-MM-dd HH:mm:ss'),sol.custom_delivered_time ) AS delivered_time,
       ost.delivered_date,
       sol.custom_delivered_time,
       date_format(ost.delivered_date, 'yyyy-MM-dd HH:mm:ss') AS logistic_delivered_time,
       ogs.confirm_time,
       ogs.collecting_time,
       ogs.shipping_time,
       ogs.shipping_abnormal_status,
       ogs.sku_collecting_status
from ods_vova_vts.ods_vova_order_info oi
         left join ods_vova_vts.ods_vova_order_goods og on og.order_id = oi.order_id
         left join ods_vova_vts.ods_vova_order_goods_status ogs on ogs.order_goods_id = og.rec_id
         left join ods_vova_vts.ods_vova_order_goods_extra oge on oge.order_goods_id = og.rec_id
      -- AND oge.collection_plan_id IN (1, 2)
         left join ods_vova_vts.ods_vova_order_shipping_tracking ost on ost.order_goods_id = og.rec_id
         left join ods_vova_vts.ods_vova_order_shipping_tracking_detail ostd on ost.tracking_id = ostd.tracking_id
         left join (select sol.order_goods_id, max(sol.create_time) AS custom_delivered_time
                    from ods_vova_vts.ods_vova_sku_ops_log sol
                    WHERE sol.ops = 'sku_shipping_status'
                      AND sol.status = 2
                      AND sol.old_status = 1
                      AND sol.worker != 'webhook'
                    group by sol.order_goods_id) sol on sol.order_goods_id = og.rec_id;
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=dwd_vova_fact_logistics" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
