#!/bin/bash
#指定日期和引擎
start_date=$1
#默认日期为昨天l
if [ ! -n "$1" ];then
start_date=`date -d "-30 day" +%Y-%m-%d`
fi
###逻辑sql
sql="
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table dwb.dwb_vova_quality_control_shipments partition(pt)
select
weekday,
ctry,
shipping_type,
is_brand,
nvl(confirm_order_cnt,0) confirm_order_cnt,
nvl(shipment_rate_5d,0) shipment_rate_5d,
nvl(online_rate_1w,0) online_rate_1w,
nvl(online_rate_2w,0) online_rate_2w,
nvl(in_warehouse_rate_1w,0) in_warehouse_rate_1w,
nvl(in_warehouse_rate_12d,0) in_warehouse_rate_12d,
substr(weekday,0,10) as pt
from
(select
weekday,
nvl(ctry,'all') ctry,
nvl(shipping_type,'all') shipping_type,
nvl(is_brand,'all') is_brand,
count(distinct order_goods_id) as confirm_order_cnt,
count(distinct(if(datediff(shipping_time,confirm_time) <5 and sku_shipping_status>=1,order_goods_id,null)))/count(distinct order_goods_id) *100 as shipment_rate_5d,
count(distinct(if(datediff(valid_tracking_date,confirm_time) <7 and to_date(valid_tracking_date)>'1970-01-01',order_goods_id,null)))/count(distinct order_goods_id) *100 as online_rate_1w,
count(distinct(if(datediff(valid_tracking_date,confirm_time) <14 and to_date(valid_tracking_date)>'1970-01-01',order_goods_id,null)))/count(distinct order_goods_id) *100 as online_rate_2w,
if(shipping_type = '集运' or shipping_type is null,concat(cast(count(distinct(if(datediff(in_warehouse_time,confirm_time) <7 and ship_status>=1,order_goods_id,null)))/count(distinct order_goods_id) *100 as decimal(13,2)),'%'),'NA') in_warehouse_rate_1w,
if(shipping_type = '集运' or shipping_type is null,concat(cast(count(distinct(if(datediff(in_warehouse_time,confirm_time) <12 and ship_status>=1,order_goods_id,null)))/count(distinct order_goods_id) *100 as decimal(13,2)),'%'),'NA') in_warehouse_rate_12d
from
(select
concat(date_sub(date(og.confirm_time),if(dayofweek(date(og.confirm_time))=1,6,dayofweek(date(og.confirm_time))-2)),'~',date_add(date(og.confirm_time),if(dayofweek(date(og.confirm_time))=1,0,8-dayofweek(date(og.confirm_time))))) as weekday,
nvl(r.region_code,'nall') ctry,
case when fl.collection_plan_id=2 then '集运'
     when fl.collection_plan_id!=2 and sc.carrier_category=3 then '平邮'
     when fl.collection_plan_id!=2 and sc.carrier_category!=3 then '非平邮'
     else 'others'
end as shipping_type,
if(dg.brand_id>0,'Y','N') is_brand,
og.order_goods_id,
og.shipping_time,
og.confirm_time,
ogs.sku_shipping_status,
ost.valid_tracking_date,
cog.in_warehouse_time,
cog.ship_status
from
dim.dim_vova_order_goods og
left join ods_vova_vts.ods_vova_order_info oi on oi.order_id = og.order_id
left join ods_vova_vts.ods_vova_region r on r.region_id = oi.country
left join dim.dim_vova_goods dg on og.goods_id = dg.goods_id
left join ods_vova_vts.ods_vova_order_goods_status ogs on og.order_goods_id = ogs.order_goods_id
left join (select order_goods_id,valid_tracking_date,shipping_carrier_id from
           (select order_goods_id,shipping_carrier_id,valid_tracking_date,
           row_number() over(partition by order_goods_id order by valid_tracking_date desc) rk
           from ods_vova_vts.ods_vova_order_shipping_tracking )
           where rk=1) ost
on og.order_goods_id = ost.order_goods_id
left join ods_vova_vts.ods_vova_collection_order_goods cog
on og.order_goods_id = cog.order_goods_id
left join ods_vova_vts.ods_vova_order_goods_extra  fl  on fl.order_goods_id = og.order_goods_id
left join ods_vova_vts.ods_vova_shipping_carrier sc on sc.carrier_id =  ost.shipping_carrier_id
where date(og.confirm_time) >= date_sub('${start_date}',dayofweek('${start_date}')-2))
group by weekday,ctry,shipping_type,is_brand with cube)
where weekday is not null


union all

select
weekday,
ctry,
shipping_type,
is_brand,
nvl(confirm_order_cnt,0) confirm_order_cnt,
nvl(shipment_rate_5d,0) shipment_rate_5d,
nvl(online_rate_1w,0) online_rate_1w,
nvl(online_rate_2w,0) online_rate_2w,
nvl(in_warehouse_rate_1w,0) in_warehouse_rate_1w,
nvl(in_warehouse_rate_12d,0) in_warehouse_rate_12d,
substr(weekday,0,10) as pt
from
(select
weekday,
nvl(ctry,'all') ctry,
'非集运' shipping_type,
nvl(is_brand,'all') is_brand,
count(distinct order_goods_id) as confirm_order_cnt,
count(distinct(if(datediff(shipping_time,confirm_time) <5 and sku_shipping_status>=1,order_goods_id,null)))/count(distinct order_goods_id) *100 as shipment_rate_5d,
count(distinct(if(datediff(valid_tracking_date,confirm_time) <7 and to_date(valid_tracking_date)>'1970-01-01',order_goods_id,null)))/count(distinct order_goods_id) *100 as online_rate_1w,
count(distinct(if(datediff(valid_tracking_date,confirm_time) <14 and to_date(valid_tracking_date)>'1970-01-01',order_goods_id,null)))/count(distinct order_goods_id) *100 as online_rate_2w,
'NA'in_warehouse_rate_1w,
'NA' in_warehouse_rate_12d
from
(select
concat(date_sub(date(og.confirm_time),if(dayofweek(date(og.confirm_time))=1,6,dayofweek(date(og.confirm_time))-2)),'~',date_add(date(og.confirm_time),if(dayofweek(date(og.confirm_time))=1,0,8-dayofweek(date(og.confirm_time))))) as weekday,
nvl(r.region_code,'nall') ctry,
case when fl.collection_plan_id=2 then '集运'
     when fl.collection_plan_id!=2 and sc.carrier_category=3 then '平邮'
     when fl.collection_plan_id!=2 and sc.carrier_category!=3 then '非平邮'
     else 'others'
end as shipping_type,
if(dg.brand_id>0,'Y','N') is_brand,
og.order_goods_id,
og.shipping_time,
og.confirm_time,
ogs.sku_shipping_status,
ost.valid_tracking_date,
cog.in_warehouse_time,
cog.ship_status
from
dim.dim_vova_order_goods og
left join ods_vova_vts.ods_vova_order_info oi on oi.order_id = og.order_id
left join ods_vova_vts.ods_vova_region r on r.region_id = oi.country
left join dim.dim_vova_goods dg on og.goods_id = dg.goods_id
left join ods_vova_vts.ods_vova_order_goods_status ogs on og.order_goods_id = ogs.order_goods_id
left join (select order_goods_id,valid_tracking_date,shipping_carrier_id from
           (select order_goods_id,shipping_carrier_id,valid_tracking_date,
           row_number() over(partition by order_goods_id order by valid_tracking_date desc) rk
           from ods_vova_vts.ods_vova_order_shipping_tracking )
           where rk=1) ost
on og.order_goods_id = ost.order_goods_id
left join ods_vova_vts.ods_vova_collection_order_goods cog
on og.order_goods_id = cog.order_goods_id
left join ods_vova_vts.ods_vova_order_goods_extra  fl  on fl.order_goods_id = og.order_goods_id
-- left join ods.vova_order_shipping_tracking ost on ost.order_goods_id = og.order_goods_id
left join ods_vova_vts.ods_vova_shipping_carrier sc on sc.carrier_id =  ost.shipping_carrier_id
where date(og.confirm_time) >= date_sub('${start_date}',dayofweek('${start_date}')-2))
where shipping_type !='集运'
group by weekday,ctry,is_brand with cube)
"

spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=30" \
--conf "spark.dynamicAllocation.initialExecutors=30" \
--conf "spark.app.name=dwb_vova_quality_control_shipments" \
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