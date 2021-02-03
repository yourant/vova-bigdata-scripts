#!/bin/bash
#指定日期和引擎
start_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
start_date=`date -d "-90 day" +%Y-%m-%d`
fi
###逻辑sql
sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwb.dwb_vova_collection_link PARTITION (pt)
select
nvl(a.weekday,'all') as weekday,
nvl(a.region_code,'all') region_code,
nvl(c.carrier_name,'all') carrier_name,
round(sum(datediff(b.logistic_delivered_time,a.confirm_time) + round((hour(b.logistic_delivered_time) - hour(a.confirm_time)) / 24,2)) / count(1),2) duan_to_duan,
percentile_approx(datediff(b.logistic_delivered_time,a.confirm_time) + round((hour(b.logistic_delivered_time) - hour(a.confirm_time)) / 24,2),0.9) duan_to_duan_90,
round(sum(datediff(e.pick_up_time,a.confirm_time) + round((hour(e.pick_up_time) - hour(a.confirm_time)) / 24,2)) / count(1),2) send_exp,
percentile_approx(datediff(e.pick_up_time,a.confirm_time) + round((hour(e.pick_up_time) - hour(a.confirm_time)) / 24,2),0.9) send_exp_90,
round(sum(datediff(e.epc_sign_time,e.pick_up_time) + round((hour(e.epc_sign_time) - hour(e.pick_up_time)) / 24,2)) / count(1),2) head_get_exp,
percentile_approx(datediff(e.epc_sign_time,e.pick_up_time) + round((hour(e.epc_sign_time) - hour(e.pick_up_time)) / 24,2),0.9) head_get_exp_90,
round(sum(datediff(f.out_warehouse_time,f.in_warehouse_time) + round((hour(f.out_warehouse_time) - hour(f.in_warehouse_time)) / 24,2)) / count(1),2) stock_in,
percentile_approx(datediff(f.out_warehouse_time,f.in_warehouse_time) + round((hour(f.out_warehouse_time) - hour(f.in_warehouse_time)) / 24,2),0.9) stock_in_90,
round(sum(datediff(f.in_warehouse_time,e.epc_sign_time) + round((hour(f.in_warehouse_time) - hour(e.epc_sign_time)) / 24,2)) / count(1),2) in_stock,
percentile_approx(datediff(f.in_warehouse_time,e.epc_sign_time) + round((hour(f.in_warehouse_time) - hour(e.epc_sign_time)) / 24,2),0.9) in_stock_90,
round(sum(datediff(f.out_warehouse_time,c.create_time) + round((hour(f.out_warehouse_time) - hour(c.create_time)) / 24,2)) / count(1),2) out_stock,
percentile_approx(datediff(f.out_warehouse_time,c.create_time) + round((hour(f.out_warehouse_time) - hour(c.create_time)) / 24,2),0.9) out_stock_90,
round(sum(datediff(c.create_time,f.in_warehouse_time) + round((hour(c.create_time) - hour(f.in_warehouse_time)) / 24,2)) / count(1),2) stay_stock,
percentile_approx(datediff(c.create_time,f.in_warehouse_time) + round((hour(c.create_time) - hour(f.in_warehouse_time)) / 24,2),0.9) stay_stock_90,
round(sum(datediff(e.arrive_dest_airport_time,f.out_warehouse_time) + round((hour(e.arrive_dest_airport_time) - hour(f.out_warehouse_time)) / 24,2)) / count(1),2) tail_main_route,
percentile_approx(datediff(e.arrive_dest_airport_time,f.out_warehouse_time) + round((hour(e.arrive_dest_airport_time) - hour(f.out_warehouse_time)) / 24,2),0.9) tail_main_route_90,
round(sum(datediff(b.logistic_delivered_time,e.transfer_last_mile_time) + round((hour(b.logistic_delivered_time) - hour(e.transfer_last_mile_time)) / 24,2)) / count(1),2) last_time,
percentile_approx(datediff(b.logistic_delivered_time,e.transfer_last_mile_time) + round((hour(b.logistic_delivered_time) - hour(e.transfer_last_mile_time)) / 24,2),0.9) last_time_90,
substr(a.weekday,0,10) as pt
from (select *,concat(date_sub(date(confirm_time),dayofweek(date(confirm_time))-2),'~',date_add(date(confirm_time),8-dayofweek(date(confirm_time)))) weekday from dim.dim_vova_order_goods) a
join dwd.dwd_vova_fact_logistics b on a.order_goods_id = b.order_goods_id
join ods_vova_vts.ods_vova_fisher_order_ship_product g on a.order_goods_id = g.order_goods_id
join (select id,get_json_object(get_json_object(detail_info, '$.notify_info'),'$.carrier_name') carrier_name,create_time from ods_vova_vts.ods_vova_fisher_order_ship) c on g.order_ship_id = c.id
join ods_vova_vts.ods_vova_order_shipping_tracking d on a.order_goods_id = d.order_goods_id
join (select * from ods_vova_ext.tmsexpo_logistics_order_tracking where data_type  = 2 and platform_id  = 2) e on d.shipping_tracking_number = e.tracking_number
join ods_vova_vts.ods_vova_collection_order_goods f on a.order_goods_id = f.order_goods_id
where date(a.confirm_time) >= date_sub('${start_date}',dayofweek('${start_date}')-2)
and a.datasource='vova'
and to_date(b.logistic_delivered_time) > '2000-01-01'
and to_date(a.confirm_time) > '2000-01-01'
and to_date(e.pick_up_time) > '2000-01-01'
and to_date(e.epc_sign_time) > '2000-01-01'
and to_date(f.out_warehouse_time) > '2000-01-01'
and to_date(f.in_warehouse_time) > '2000-01-01'
and to_date(c.create_time) > '2000-01-01'
and to_date(e.transfer_last_mile_time) > '2000-01-01'
and to_date(e.arrive_dest_airport_time) > '2000-01-01'
and datediff(e.epc_sign_time,e.pick_up_time) + round((hour(e.epc_sign_time) - hour(e.pick_up_time)) / 24,2) > 0
and datediff(f.out_warehouse_time,f.in_warehouse_time) + round((hour(f.out_warehouse_time) - hour(f.in_warehouse_time)) / 24,2) > 0
group by cube(a.weekday,a.region_code,c.carrier_name)
"

spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=30" \
--conf "spark.dynamicAllocation.initialExecutors=30" \
--conf "spark.app.name=dwb_vova_collection_link" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi





