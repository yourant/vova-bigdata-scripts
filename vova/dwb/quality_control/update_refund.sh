#!/bin/bash
#指定日期和引擎
start_date=$1
#默认日期为昨天l
if [ ! -n "$1" ];then
start_date=`date -d "-90 day" +%Y-%m-%d`
fi
###逻辑sql
sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwb.dwb_vova_quality_control_refund partition(pt)
select
nvl(weekday,'all') weekday,
nvl(region_code,'all') ctry,
sum(is_refund) as refund_order_cnt,
nvl(sum(is_refund_9w)/count(1),0)*100 as refund_9w_rate,
nvl(sum(is_refund_12w)/count(1),0)*100 as refund_12w_rate,
nvl(sum(is_refund_15w)/count(1),0)*100 as refund_15w_rate,
nvl(sum(is_refund_15w_lrf)/count(1),0)*100 as refund_15w_lrf_rate,
nvl(sum(is_refund_15w_nlrf)/count(1),0)*100 as refund_15w_nlrf_rate,
sum(complaint_15w) as complaint_cnt,
nvl(sum(complaint_15w)/count(1),0)*100 as complaint_rate,
nvl(sum(if(complaint_15w=1 and is_delivered_out_time=1,1,0))/sum(complaint_15w),0)*100 as complaint_delivered_out_time_rate,
nvl(sum(is_cancal_15w)/count(*),0)*100 as cancal_rate,
nvl(sum(platform_refund_15w)/count(*),0)*100 as platform_refund_15w_rate,
nvl(sum(mct_refund_15w)/count(*),0)*100 as mct_refund_15w_rate,
substr(weekday,0,10) as pt
from
(select
concat(date_sub(date(fp.pay_time),if(dayofweek(date(fp.pay_time))=1,6,dayofweek(date(fp.pay_time))-2)),'~',date_add(date(fp.pay_time),if(dayofweek(date(fp.pay_time))=1,0,8-dayofweek(date(fp.pay_time))))) as weekday,
nvl(fp.region_code,'NALL') as region_code,
fr.order_goods_id,
if(fr.refund_type_id not in (1,7)  and ogs.sku_pay_status = 4,1,0) as is_refund,
if(fr.refund_type_id not in (1,7)  and ogs.sku_pay_status = 4 and datediff(fr.exec_refund_time,fp.pay_time) <63,1,0) as is_refund_9w,
if(fr.refund_type_id not in (1,7)  and ogs.sku_pay_status = 4 and datediff(fr.exec_refund_time,fp.pay_time) <84,1,0) as is_refund_12w,
if(fr.refund_type_id not in (1,7)  and ogs.sku_pay_status = 4 and datediff(fr.exec_refund_time,fp.pay_time) <105,1,0) as is_refund_15w,
if(fr.refund_type_id not in (1,7)  and ogs.sku_pay_status = 4 and datediff(fr.exec_refund_time,fp.pay_time) <105 and fr.refund_reason_type_id not in (8,9) ,1,0) as is_refund_15w_nlrf,
if(fr.refund_type_id not in (1,7)  and ogs.sku_pay_status = 4 and datediff(fr.exec_refund_time,fp.pay_time) <105 and fr.refund_reason_type_id in (8,9),1,0) as is_refund_15w_lrf,
if(fr.recheck_type =2 and fl.delivered_time is not null and datediff(fl.delivered_time,fp.pay_time) <105 ,1,0) complaint_15w,
if(fl.delivered_time is not null and fl.delivered_time > oge.latest_delivery_time,1,0 ) as is_delivered_out_time,
if(og.sku_order_status=2 and fr.refund_type_id not in (2,12) and datediff(fr.create_time,fp.pay_time)<105 ,1,0 ) as is_cancal_15w,
if(fr.refund_type_id in (3,4,8,9,10,13)  and ogs.sku_pay_status = 4 and datediff(fr.exec_refund_time,fp.pay_time) <105,1,0) as platform_refund_15w,
if(fr.refund_type_id in (5,6,11,14)  and ogs.sku_pay_status = 4 and datediff(fr.exec_refund_time,fp.pay_time) <105,1,0) as mct_refund_15w
from
dwd.dwd_vova_fact_pay fp
left join dwd.dwd_vova_fact_refund fr
on fp.order_goods_id = fr.order_goods_id
left join ods_vova_vts.ods_vova_order_goods_status ogs on fp.order_goods_id = ogs.order_goods_id
left join dwd.dwd_vova_fact_logistics fl  on fl.order_goods_id = fp.order_goods_id
left join (select rec_id,from_unixtime(extension_info)  as latest_delivery_time from  ods_vova_vts.ods_vova_order_goods_extension where ext_name='latest_delivery_time') oge on fp.order_goods_id = oge.rec_id
left join dim.dim_vova_order_goods og on fp.order_goods_id = og.order_goods_id
where date(fp.pay_time) >= date_sub('${start_date}',dayofweek('${start_date}')-2)
and fp.datasource='vova')
group by
weekday,
region_code
grouping sets(
(weekday,region_code),
(weekday)
)
"

spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=30" \
--conf "spark.dynamicAllocation.initialExecutors=30" \
--conf "spark.app.name=dwb_vova_quality_control_refund" \
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