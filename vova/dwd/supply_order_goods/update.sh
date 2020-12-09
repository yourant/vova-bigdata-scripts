#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
sql="
insert overwrite table dwd.dwd_vova_fact_supply_order_goods
select
tpog.order_goods_id,
tpog.order_id,
tpog.channel_order_goods_sn,
owi.purchase_waybill_no AS last_waybill_no,
tpog.order_goods_shipping_status,
tpogpp.purchase_platform_order_id,
tpogpp.purchase_platform_parent_order_id,
tpogpp.purchase_order_goods_id,
tpogpp.purchase_order_status,
tpogpp.purchase_order_pay_status,
tpogpp.purchase_order_shipping_status,
tpogpp.in_inventory_status,
tpogpp.purchase_amount,
pog.purchase_order_id,
wi.purchase_shipping_time,
wi.inbound_weight, --入库重量单位g,订单粒度
wi.bound_status, --出入库状态:未入库1,已入库10
wi.purchase_waybill_no, --采购渠道运单号
owi.plan_freight, --预计运费,运单粒度
owi.actual_freight, --实际运费,运单粒度
owi.actual_weight, --实际包裹重量 单位g,运单粒度
owi.guess_weight, --出库重量,运单粒度
owi.carrier_code, --承运商
owi.refer_waybill_no
from
ods_vova_pangu.ods_vova_order_goods tpog
INNER JOIN ods_vova_pangu.ods_vova_order_info tpoi ON tpoi.order_id = tpog.order_id
LEFT JOIN
         (
         select
         t1.purchase_platform_order_id,
         t1.purchase_platform_parent_order_id,
         t1.purchase_order_goods_id,
         t1.purchase_order_status,
         t1.purchase_order_pay_status,
         t1.purchase_order_shipping_status,
         t1.in_inventory_status,
         t1.purchase_amount,
         t1.order_goods_id
         from
         (
         select
         tpogpp.purchase_platform_order_id,
         tpogpp.purchase_platform_parent_order_id,
         tpogpp.purchase_order_goods_id,
         tpogpp.purchase_order_status,
         tpogpp.purchase_order_pay_status,
         tpogpp.purchase_order_shipping_status,
         tpogpp.in_inventory_status,
         tpogpp.purchase_amount,
         tpogpp.order_goods_id,
         row_number() over (partition by order_goods_id order by create_time desc)        as rank
         from
         ods_vova_pangu.ods_vova_order_goods_purchase_plan tpogpp
         ) t1
         where t1.rank = 1
         ) tpogpp ON tpogpp.order_goods_id = tpog.order_goods_id
LEFT JOIN ods_vova_pangu.ods_vova_purchase_order_goods pog ON tpogpp.purchase_order_goods_id = pog.purchase_order_goods_id
LEFT JOIN ods_vova_pangu.ods_vova_purchase_order_info poi ON pog.purchase_order_id = poi.purchase_order_id
LEFT JOIN
 (
 select
   wi.purchase_order_goods_id,
   first(wi.purchase_shipping_time) AS purchase_shipping_time,
   first(wi.purchase_waybill_no) AS purchase_waybill_no,
   first(wi.bound_status) AS bound_status,
   sum(wi.inbound_weight) AS inbound_weight
 from
 ods_vova_pangu.ods_vova_waybill_info wi
 GROUP BY wi.purchase_order_goods_id
 ) wi
 ON wi.purchase_order_goods_id = pog.purchase_order_goods_id
LEFT JOIN ods_vova_pangu.ods_vova_outbound_waybill_info owi ON tpog.refer_waybill_no = owi.refer_waybill_no
;
"

#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=dwd_vova_fact_supply_order_goods" -e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
