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
insert overwrite table dwb.dwb_vova_quality_control_delivered partition(pt)
select
nvl(weekday,'all') weekday,
nvl(region_code,'all') ctry,
sum(if(delivered_gap<84,1,0))/count(1)*100 as delivered_rate_12w,
sum(if(delivered_gap<84 and is_collection_ship = 1, 1,0))/sum(if(is_collection_ship = 1, 1,0))*100 as collection_delivered_rate_12w,
sum(if(is_not_col_not_pingyou = 1 and delivered_gap<84,1,0))/sum(if(is_not_col_not_pingyou = 1, 1,0))*100 as not_col_not_pingyou_delivered_rate_12w,
avg(if(is_collection_ship = 1 and delivered_gap<84,delivered_gap,null)) as collection_delivered_time_avg,
avg(if(is_not_col_not_pingyou = 1 and delivered_gap<84,delivered_gap,null)) as not_col_not_pingyou_delivered_time_avg,
substr(weekday,0,10) as pt
from
(select
concat(date_sub(date(fp.pay_time),if(dayofweek(date(fp.pay_time))=1,6,dayofweek(date(fp.pay_time))-2)),'~',date_add(date(fp.pay_time),if(dayofweek(date(fp.pay_time))=1,0,8-dayofweek(date(fp.pay_time))))) as weekday,
nvl(fp.region_code,'NALL') as region_code,
if(fl.collection_plan_id = 0  and sc.carrier_category is not null and sc.carrier_category!=3,1,0) as is_not_col_not_pingyou,
if(fl.collection_plan_id IN (1, 2),1,0) is_collection_ship,
(cast(if(date(fl.delivered_time)>'2000-01-01',fl.delivered_time,null) as bigint)-cast(fp.pay_time as bigint))/(60*60*24) as delivered_gap
from
dwd.dwd_vova_fact_pay fp
left join dwd.dwd_vova_fact_logistics fl  on fl.order_goods_id = fp.order_goods_id
left join ods_vova_vts.ods_vova_shipping_carrier sc on sc.carrier_id =  fl.shipping_carrier_id
left join ods_vova_vts.ods_vova_order_goods_status ogs on fp.order_goods_id = ogs.order_goods_id
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
--conf "spark.app.name=dwb_vova_quality_control_delivered" \
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