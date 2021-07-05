#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ads.ads_vova_refund_analysis partition(pt)
select
/*+ repartition(30) */
og.order_id,
og.order_goods_id,
og.datasource,
og.region_code as country,
case when fl.collection_plan_id=2 then '集运'
     when fl.collection_plan_id!=2 and sc.carrier_category=3 then '平邮'
     when fl.collection_plan_id!=2 and sc.carrier_category!=3 then '非平邮'
     else 'others' end as shipping_channel,
og.goods_id,
dg.first_cat_id,
dg.first_cat_name,
if(dg.brand_id>0,'Y','N') as is_brand,
dg.mct_id,
dm.mct_name,
mr.rank as mct_rank,
fr.refund_type,
fr.refund_type_id,
fr.refund_reason as refund_reason_type,
fr.refund_reason_type_id,
case when fr.rr_audit_status = 'audit_passed' or ogs.sku_pay_status = 4 then 'Y'
     when fr.rr_audit_status is not null then 'N'
     end is_refund_passed,
if(date(delivered_time)>='2000-01-01','Y','N')     as is_delivered,
og.pay_time,
if(date(og.shipping_time)<'2000-01-01',null,og.shipping_time) as valid_tracking_date,
og.confirm_time,
if(date(fl.delivered_time)<'2000-01-01',null,fl.delivered_time) as valid_tracking_date,
if(date(ost.valid_tracking_date)<'2000-01-01',null,ost.valid_tracking_date) as valid_tracking_date,
ogs.sku_pay_status as pay_status,
og.sku_order_status,
og.sku_shipping_status,
og.goods_number*og.shop_price+og.shipping_fee as amount,
date(og.confirm_time) as pt
from
dim.dim_vova_order_goods og
left join (select order_goods_id,valid_tracking_date,shipping_carrier_id from
           (select order_goods_id,shipping_carrier_id,valid_tracking_date,
           row_number() over(partition by order_goods_id order by valid_tracking_date desc) rk
           from ods_vova_vts.ods_vova_order_shipping_tracking )
           where rk=1) ost
on og.order_goods_id = ost.order_goods_id
left join  dwd.dwd_vova_fact_logistics  fl  on fl.order_goods_id = og.order_goods_id
left join ods_vova_vts.ods_vova_shipping_carrier sc on sc.carrier_id =  ost.shipping_carrier_id
left join dim.dim_vova_goods dg on og.goods_id = dg.goods_id
left join dim.dim_vova_merchant dm on dg.mct_id = dm.mct_id
left join  ads.ads_vova_mct_rank mr on dg.mct_id = mr.mct_id and dg.first_cat_id = mr.first_cat_id and mr.pt='${pre_date}'
left join dwd.dwd_vova_fact_refund fr on og.order_goods_id =fr.order_goods_id
left join ods_vova_vts.ods_vova_order_goods_status ogs on og.order_goods_id = ogs.order_goods_id
where date(og.confirm_time)>='2021-01-01'
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=ads_vova_goods_analysis" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=120" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.autoBroadcastJoinThreshold=-1" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
