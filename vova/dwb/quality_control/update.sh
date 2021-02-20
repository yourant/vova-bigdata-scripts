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
insert overwrite table dwb.dwb_vova_quality_control_core_tag partition(pt)
select
weekday,
region_code,
not_pingyou_deliverd_rate,
collect_deliverd_rate,
deliverd_time_avg,
refund_rate,
complaint_rate,
cancal_rate,
cb_rate,
pt
from
(select
nvl(weekday,'all') as weekday,
nvl(region_code,'all') as region_code,
nvl(sum(if(is_not_pingyou=1 and process_tag='Delivered' and is_delivered_on_time=1 , 1,0))/sum(if(is_not_pingyou=1 , 1,0))*100,0) as not_pingyou_deliverd_rate,
nvl(sum(if(process_tag='Delivered' and collection_plan_id in (1,2)  and is_delivered_on_time=1 , 1,0))/sum(if(collection_plan_id in (1,2),1,0))*100,0) as collect_deliverd_rate,
nvl(avg(if(process_tag='Delivered' and delivered_gap>0,delivered_gap,null )),0) as deliverd_time_avg,
nvl(sum(is_refund_9w)/count(*)*100,0) as refund_rate,
nvl(sum(complaint_90d)/count(*)*100,0) as complaint_rate,
nvl(sum(is_cancal_2w)/count(*)*100,0) as cancal_rate,
nvl(sum(is_cb)/count(*)*100,0) as cb_rate,
substr(weekday,0,10) as pt
from
(select
nvl(fp.region_code,'NALL') as region_code,
fp.order_goods_id,
fl.process_tag,
fl.collection_plan_id,
if(sc.carrier_category!=3,1,0) as is_not_pingyou,
if(fl.delivered_time is not null and fl.delivered_time<=oge.latest_delivery_time,1,0 ) as is_delivered_on_time,
(cast(fl.delivered_time as bigint)-cast(fp.pay_time as bigint))/(60*60*24) as delivered_gap,
if(fr.refund_type_id not in (1,7)  and ogs.sku_pay_status > 1 and ((fr.rr_audit_status = 'audit_passed' and datediff(fr.rr_audit_time,fp.confirm_time)<63) or (fr.rr_audit_status != 'audit_passed' and ogs.sku_pay_status = 4 and datediff(fr.exec_refund_time,fp.confirm_time)<63))  ,1,0) as is_refund_9w,
if(fr.recheck_type =2 and fl.delivered_time is not null and datediff(fl.delivered_time,fp.pay_time) <90   ,1,0) complaint_90d,
if(og.sku_order_status=2 and fr.refund_type_id not in (2,12) and datediff(fr.create_time,fp.pay_time)<14 ,1,0 ) as is_cancal_2w,
if(cr.track_id is not null and cr.create_time>=fp.pay_time and datediff(cr.create_time,fp.pay_time) <60 ,1,0) is_cb,
concat(date_sub(date(fp.pay_time),if(dayofweek(date(fp.pay_time))=1,6,dayofweek(date(fp.pay_time))-2)),'~',date_add(date(fp.pay_time),if(dayofweek(date(fp.pay_time))=1,0,8-dayofweek(date(fp.pay_time))))) as weekday
from
dwd.dwd_vova_fact_pay fp
left join
dwd.dwd_vova_fact_logistics fl  on fl.order_goods_id = fp.order_goods_id
left join ods_vova_vts.ods_vova_shipping_carrier sc on sc.carrier_id =  fl.shipping_carrier_id
left join ods_vova_vts.ods_vova_collection_order_goods cog on fp.order_goods_id = cog.order_goods_id
left join dwd.dwd_vova_fact_refund fr on fp.order_goods_id = fr.order_goods_id
left join dim.dim_vova_order_goods og on fp.order_goods_id = og.order_goods_id
left join (select track_id,min(create_time) as create_time from ods_vova_vts.ods_vova_checkout_chargeback_reporting group by track_id) cr on og.order_sn = cr.track_id
left join (select rec_id,from_unixtime(extension_info)  as latest_delivery_time from  ods_vova_vts.ods_vova_order_goods_extension where ext_name='latest_delivery_time') oge on fp.order_goods_id = oge.rec_id
left join ods_vova_vts.ods_vova_order_goods_status ogs on og.order_goods_id = ogs.order_goods_id
where date(fp.pay_time) >= date_sub('${start_date}',dayofweek('${start_date}')-2) and fp.datasource='vova')
group by weekday,region_code with cube)
where weekday != 'all' and pt is not null
"

spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=30" \
--conf "spark.dynamicAllocation.initialExecutors=30" \
--conf "spark.app.name=dwb_vova_quality_control_core_tag" \
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