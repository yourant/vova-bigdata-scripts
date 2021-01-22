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
insert overwrite table dwb.dwb_vova_order_goods_life_cycle_type partition(pt)
select
/*+ repartition(1) */
nvl(nvl(og.region_code,'NALL'),'ALL') as ctry,
'ALL' as type,
count(*) order_cnt,
count(fp.order_goods_id) as pay_cnt,
count(if(og.sku_order_status = 1 AND ogs.sku_pay_status IN ( 2, 11 ) ,og.order_goods_id,null)) as confirm_cnt,
count(if(og.sku_order_status = 2 AND ogs.sku_pay_status>0 AND fr.refund_type_id!=2 ,og.order_goods_id,null)) as cancel_cnt,
count(if(ogs.sku_shipping_status=0 AND og.sku_order_status = 1  AND ogs.sku_pay_status IN ( 2, 11 ) ,og.order_goods_id,null)) as unfilled_cnt,
count(if(ogs.sku_shipping_status>=1,og.order_goods_id,null)) as mark_cnt,
count(ip.order_goods_id) as reported_cnt,
count(if(to_date(ost.valid_tracking_date)>'1970-01-01',og.order_goods_id,null)) as online_cnt,
count(if(cog.ship_status>=1,og.order_goods_id,null)) as in_warehouse_cnt,
count(if(cog.ship_status in (2,4,5),og.order_goods_id,null)) as push_warehouse_cnt,
count(if(cog.ship_status = 5,og.order_goods_id,null)) as fact_out_warehouse_cnt,
count(if(ost.process_tag='Delivered',og.order_goods_id,null)) as delivered_cnt,
count(if(ogs.sku_shipping_status=2 and ost.process_tag!='Delivered',og.order_goods_id,null)) as confirm_not_delivered_cnt,
count(if(ogs.sku_shipping_status!=2 and ost.process_tag!='Delivered',og.order_goods_id,null)) as not_confirm_not_delivered_cnt,
count(if(ogs.sku_pay_status >0 AND fr.refund_type_id=2,fr.order_goods_id, null)) as apply_refund_cnt,
count(if(fr.order_goods_id is not null AND ogs.sku_pay_status =4 AND fr.refund_type_id=2,og.order_goods_id,null)) as refunded_cnt,
nvl(to_date(og.order_time),'ALL') as pt
from
dim.dim_vova_order_goods og
left join dwd.dwd_vova_fact_pay fp
on og.order_goods_id=fp.order_goods_id
left join ods_vova_vts.ods_vova_order_goods_status ogs
on og.order_goods_id = ogs.order_goods_id
left join (select order_goods_id from ods_vova_vts.ods_vova_fisher_order_info_product group by order_goods_id) ip
on og.order_goods_id = ip.order_goods_id
left join (select order_goods_id,valid_tracking_date,process_tag from
           (select order_goods_id,valid_tracking_date,process_tag,
           row_number() over(partition by order_goods_id order by valid_tracking_date desc) rk
           from ods_vova_vts.ods_vova_order_shipping_tracking )
           where rk=1) ost
on og.order_goods_id = ost.order_goods_id
left join ods_vova_vts.ods_vova_collection_order_goods cog
on og.order_goods_id = cog.order_goods_id
left join dwd.dwd_vova_fact_refund fr
on og.order_goods_id = fr.order_goods_id
where to_date(og.order_time)>=date_sub('${cur_date}',90) and to_date(og.order_time)<='${cur_date}'
group by
to_date(og.order_time),
nvl(og.region_code,'NALL')
with cube
having pt != 'ALL'

union all
select
nvl(nvl(og.region_code,'NALL'),'ALL') as ctry,
'捷网集运' as type,
'NA' order_cnt,
'NA' as pay_cnt,
count(if(og.sku_order_status = 1 AND ogs.sku_pay_status IN ( 2, 11 ) ,og.order_goods_id,null)) as confirm_cnt,
count(if(og.sku_order_status = 2 AND ogs.sku_pay_status>0 AND fr.refund_type_id!=2 ,og.order_goods_id,null)) as cancel_cnt,
count(if(ogs.sku_shipping_status=0 AND og.sku_order_status = 1  AND ogs.sku_pay_status IN ( 2, 11 ) ,og.order_goods_id,null)) as unfilled_cnt,
count(if(ogs.sku_shipping_status>=1,og.order_goods_id,null)) as mark_cnt,
count(ip.order_goods_id) as reported_cnt,
count(if(to_date(ost.valid_tracking_date)>'1970-01-01',og.order_goods_id,null)) as online_cnt,
count(if(cog.ship_status>=1,og.order_goods_id,null)) as in_warehouse_cnt,
count(if(cog.ship_status in (2,4,5),og.order_goods_id,null)) as push_warehouse_cnt,
count(if(cog.ship_status = 5,og.order_goods_id,null)) as fact_out_warehouse_cnt,
count(if(ost.process_tag='Delivered',og.order_goods_id,null)) as delivered_cnt,
count(if(ogs.sku_shipping_status=2 and ost.process_tag!='Delivered',og.order_goods_id,null)) as confirm_not_delivered_cnt,
count(if(ogs.sku_shipping_status!=2 and ost.process_tag!='Delivered',og.order_goods_id,null)) as not_confirm_not_delivered_cnt,
count(if(ogs.sku_pay_status >0 AND fr.refund_type_id=2,fr.order_goods_id, null)) as apply_refund_cnt,
count(if(fr.order_goods_id is not null AND ogs.sku_pay_status =4 AND fr.refund_type_id=2,og.order_goods_id,null)) as refunded_cnt,
nvl(to_date(og.order_time),'ALL') as pt
from
dim.dim_vova_order_goods og
left join dwd.dwd_vova_fact_pay fp
on og.order_goods_id=fp.order_goods_id
left join ods_vova_vts.ods_vova_order_goods_status ogs
on og.order_goods_id = ogs.order_goods_id
left join (select order_goods_id from ods_vova_vts.ods_vova_fisher_order_info_product group by order_goods_id) ip
on og.order_goods_id = ip.order_goods_id
left join (select order_goods_id,valid_tracking_date,process_tag from
           (select order_goods_id,valid_tracking_date,process_tag,
           row_number() over(partition by order_goods_id order by valid_tracking_date desc) rk
           from ods_vova_vts.ods_vova_order_shipping_tracking )
           where rk=1) ost
on og.order_goods_id = ost.order_goods_id
left join ods_vova_vts.ods_vova_collection_order_goods cog
on og.order_goods_id = cog.order_goods_id
left join dwd.dwd_vova_fact_refund fr
on og.order_goods_id = fr.order_goods_id
left join ods_vova_vts.ods_vova_order_goods_extra ge
on og.order_goods_id = ge.order_goods_id
where to_date(og.order_time)>=date_sub('${cur_date}',90)
and to_date(og.order_time)<='${cur_date}'
and cog.combine_type in (2, 3)
group by
to_date(og.order_time),
nvl(og.region_code,'NALL')
with cube
having pt != 'ALL'

union all
select
nvl(nvl(og.region_code,'NALL'),'ALL') as ctry,
'燕文集运' as type,
'NA' order_cnt,
'NA' as pay_cnt,
count(if(og.sku_order_status = 1 AND ogs.sku_pay_status IN ( 2, 11 ) ,og.order_goods_id,null)) as confirm_cnt,
count(if(og.sku_order_status = 2 AND ogs.sku_pay_status>0 AND fr.refund_type_id!=2 ,og.order_goods_id,null)) as cancel_cnt,
count(if(ogs.sku_shipping_status=0 AND og.sku_order_status = 1 AND ogs.sku_pay_status IN ( 2, 11 ),og.order_goods_id,null)) as unfilled_cnt,
count(if(ogs.sku_shipping_status>=1,og.order_goods_id,null)) as mark_cnt,
'NA' as reported_cnt,
count(if(to_date(ost.valid_tracking_date)>'1970-01-01',og.order_goods_id,null)) as online_cnt,
'NA' as in_warehouse_cnt,
'NA' as push_warehouse_cnt,
'NA' as fact_out_warehouse_cnt,
count(if(ost.process_tag='Delivered',og.order_goods_id,null)) as delivered_cnt,
count(if(ogs.sku_shipping_status=2 and ost.process_tag!='Delivered',og.order_goods_id,null)) as confirm_not_delivered_cnt,
count(if(ogs.sku_shipping_status!=2 and ost.process_tag!='Delivered',og.order_goods_id,null)) as not_confirm_not_delivered_cnt,
count(if(ogs.sku_pay_status >0 AND fr.refund_type_id=2,fr.order_goods_id, null)) as apply_refund_cnt,
count(if(fr.order_goods_id is not null AND ogs.sku_pay_status =4 AND fr.refund_type_id=2,og.order_goods_id,null)) as refunded_cnt,
nvl(to_date(og.order_time),'ALL') as pt
from
dim.dim_vova_order_goods og
left join dwd.dwd_vova_fact_pay fp
on og.order_goods_id=fp.order_goods_id
left join ods_vova_vts.ods_vova_order_goods_status ogs
on og.order_goods_id = ogs.order_goods_id
left join (select order_goods_id from ods_vova_vts.ods_vova_fisher_order_info_product group by order_goods_id) ip
on og.order_goods_id = ip.order_goods_id
left join (select order_goods_id,valid_tracking_date,process_tag from
           (select order_goods_id,valid_tracking_date,process_tag,
           row_number() over(partition by order_goods_id order by valid_tracking_date desc) rk
           from ods_vova_vts.ods_vova_order_shipping_tracking )
           where rk=1) ost
on og.order_goods_id = ost.order_goods_id
left join ods_vova_vts.ods_vova_collection_order_goods cog
on og.order_goods_id = cog.order_goods_id
left join dwd.dwd_vova_fact_refund fr
on og.order_goods_id = fr.order_goods_id
left join ods_vova_vts.ods_vova_order_goods_extra ge
on og.order_goods_id = ge.order_goods_id
where to_date(og.order_time)>=date_sub('${cur_date}',90)
and to_date(og.order_time)<='${cur_date}'
and cog.combine_type = 1
group by
to_date(og.order_time),
nvl(og.region_code,'NALL')
with cube
having pt != 'ALL'

union all
select
nvl(nvl(og.region_code,'NALL'),'ALL') as ctry,
'普通' as type,
'NA' order_cnt,
'NA' as pay_cnt,
count(if(og.sku_order_status = 1 AND ogs.sku_pay_status IN ( 2, 11 ) ,og.order_goods_id,null)) as confirm_cnt,
count(if(og.sku_order_status = 2 AND ogs.sku_pay_status>0 AND fr.refund_type_id!=2 ,og.order_goods_id,null)) as cancel_cnt,
count(if(ogs.sku_shipping_status=0 AND og.sku_order_status = 1 AND ogs.sku_pay_status IN ( 2, 11 ),og.order_goods_id,null)) as unfilled_cnt,
count(if(ogs.sku_shipping_status>=1,og.order_goods_id,null)) as mark_cnt,
'NA' as reported_cnt,
count(if(to_date(ost.valid_tracking_date)>'1970-01-01',og.order_goods_id,null)) as online_cnt,
'NA' as in_warehouse_cnt,
'NA' as push_warehouse_cnt,
'NA' as fact_out_warehouse_cnt,
count(if(ost.process_tag='Delivered',og.order_goods_id,null)) as delivered_cnt,
count(if(ogs.sku_shipping_status=2 and ost.process_tag!='Delivered',og.order_goods_id,null)) as confirm_not_delivered_cnt,
count(if(ogs.sku_shipping_status!=2 and ost.process_tag!='Delivered',og.order_goods_id,null)) as not_confirm_not_delivered_cnt,
count(if(ogs.sku_pay_status >0 AND fr.refund_type_id=2,fr.order_goods_id, null)) as apply_refund_cnt,
count(if(fr.order_goods_id is not null AND ogs.sku_pay_status =4 AND fr.refund_type_id=2,og.order_goods_id,null)) as refunded_cnt,
nvl(to_date(og.order_time),'ALL') as pt
from
dim.dim_vova_order_goods og
left join dwd.dwd_vova_fact_pay fp
on og.order_goods_id=fp.order_goods_id
left join ods_vova_vts.ods_vova_order_goods_status ogs
on og.order_goods_id = ogs.order_goods_id
left join (select order_goods_id from ods_vova_vts.ods_vova_fisher_order_info_product group by order_goods_id) ip
on og.order_goods_id = ip.order_goods_id
left join (select order_goods_id,valid_tracking_date,process_tag from
           (select order_goods_id,valid_tracking_date,process_tag,
           row_number() over(partition by order_goods_id order by valid_tracking_date desc) rk
           from ods_vova_vts.ods_vova_order_shipping_tracking )
           where rk=1) ost
on og.order_goods_id = ost.order_goods_id
left join ods_vova_vts.ods_vova_collection_order_goods cog
on og.order_goods_id = cog.order_goods_id
left join dwd.dwd_vova_fact_refund fr
on og.order_goods_id = fr.order_goods_id
left join ods_vova_vts.ods_vova_order_goods_extra ge
on og.order_goods_id = ge.order_goods_id
where to_date(og.order_time)>=date_sub('${cur_date}',90)
and to_date(og.order_time)<='${cur_date}'
and ge.collection_plan_id =0
group by
to_date(og.order_time),
nvl(og.region_code,'NALL')
with cube
having pt != 'ALL'

union all
select
nvl(nvl(og.region_code,'NALL'),'ALL') as ctry,
'前置仓' as type,
'NA' order_cnt,
'NA' as pay_cnt,
count(if(og.sku_order_status = 1 AND ogs.sku_pay_status IN ( 2, 11 ) ,og.order_goods_id,null)) as confirm_cnt,
count(if(og.sku_order_status = 2 AND ogs.sku_pay_status>0 AND fr.refund_type_id!=2 ,og.order_goods_id,null)) as cancel_cnt,
count(if(ogs.sku_shipping_status=0 AND og.sku_order_status = 1 AND ogs.sku_pay_status IN ( 2, 11 ),og.order_goods_id,null)) as unfilled_cnt,
count(if(ogs.sku_shipping_status>=1,og.order_goods_id,null)) as mark_cnt,
'NA' as reported_cnt,
count(if(to_date(ost.valid_tracking_date)>'1970-01-01',og.order_goods_id,null)) as online_cnt,
'NA' as in_warehouse_cnt,
'NA' as push_warehouse_cnt,
'NA' as fact_out_warehouse_cnt,
count(if(ost.process_tag='Delivered',og.order_goods_id,null)) as delivered_cnt,
count(if(ogs.sku_shipping_status=2 and ost.process_tag!='Delivered',og.order_goods_id,null)) as confirm_not_delivered_cnt,
count(if(ogs.sku_shipping_status!=2 and ost.process_tag!='Delivered',og.order_goods_id,null)) as not_confirm_not_delivered_cnt,
count(if(ogs.sku_pay_status >0 AND fr.refund_type_id=2,fr.order_goods_id, null)) as apply_refund_cnt,
count(if(fr.order_goods_id is not null AND ogs.sku_pay_status =4 AND fr.refund_type_id=2,og.order_goods_id,null)) as refunded_cnt,
nvl(to_date(og.order_time),'ALL') as pt
from
dim.dim_vova_order_goods og
left join dwd.dwd_vova_fact_pay fp
on og.order_goods_id=fp.order_goods_id
left join ods_vova_vts.ods_vova_order_goods_status ogs
on og.order_goods_id = ogs.order_goods_id
left join (select order_goods_id from ods_vova_vts.ods_vova_fisher_order_info_product group by order_goods_id) ip
on og.order_goods_id = ip.order_goods_id
left join (select order_goods_id,valid_tracking_date,process_tag from
           (select order_goods_id,valid_tracking_date,process_tag,
           row_number() over(partition by order_goods_id order by valid_tracking_date desc) rk
           from ods_vova_vts.ods_vova_order_shipping_tracking )
           where rk=1) ost
on og.order_goods_id = ost.order_goods_id
left join ods_vova_vts.ods_vova_collection_order_goods cog
on og.order_goods_id = cog.order_goods_id
left join dwd.dwd_vova_fact_refund fr
on og.order_goods_id = fr.order_goods_id
left join ods_vova_vts.ods_vova_order_goods_extra ge
on og.order_goods_id = ge.order_goods_id
where to_date(og.order_time)>=date_sub('${cur_date}',90)
and to_date(og.order_time)<='${cur_date}'
and ge.storage_type = 3
group by
to_date(og.order_time),
nvl(og.region_code,'NALL')
with cube
having pt != 'ALL'
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=rpt_order_goods_life_cycle_type" \
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