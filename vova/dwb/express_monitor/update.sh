#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-12 day" +%Y-%m-%d`
fi

spark-sql  --conf "spark.app.name=rpt_express_monitor"  --conf "spark.dynamicAllocation.maxExecutors=100" --conf "spark.sql.crossJoin.enabled=true" -e "

set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=1000;

insert overwrite table rpt.rpt_logistics_order_monitor   PARTITION (pt)
select
a.pt cur_date,
a.region_code,
a.collect_type,
a.logistics_cnt, --集运单数
concat(nvl(round(a.logistics_cnt * 100 / b.logistics_cnt,2),0),'%') logistics_cnt_rate, --渗透率
concat(nvl(round(a.is_4_day_pick_up * 100 / a.logistics_cnt,2),0),'%') 4_day_pick_up_rate, --4天上线率
concat(nvl(round(a.is_7_day_in_warehouse * 100 / a.logistics_cnt,2),0),'%') 7_day_in_warehouse_rate, --7天入库率
concat(nvl(round(a.is_9_day_out_warehouse * 100 / a.logistics_cnt,2),0),'%') 9_day_out_warehouse_rate, --9天出库率
concat(nvl(round(a.is_12_day_refund * 100 / a.logistics_cnt,2),0),'%') 12_day_refund_rate --12天取消率
,'',a.pt pt
from (
SELECT
  nvl(nvl(r.region_code,'NA'),'all') region_code,
  nvl(if(cog.logistics_product_id = 2,'自寄','物流商揽收'),'all') collect_type, --揽收方式
  nvl(to_date(ogs.confirm_time),'all') pt,
  count(distinct ogs.order_goods_id) logistics_cnt, --集运单数
  count(distinct case when lolt1.pickup_time is null or unix_timestamp(lolt1.pickup_time,'yyyy-MM-dd HH:mm:ss') < unix_timestamp(ogs.confirm_time,'yyyy-MM-dd HH:mm:ss')  then null else case when (unix_timestamp(lolt1.pickup_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(ogs.confirm_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 / 24 < 4 then ogs.order_goods_id else null end end) is_4_day_pick_up, --4天上线
  count(distinct case when lolt1.in_warehouse_time is null or unix_timestamp(lolt1.in_warehouse_time,'yyyy-MM-dd HH:mm:ss') < unix_timestamp(ogs.confirm_time,'yyyy-MM-dd HH:mm:ss') then null else case when (unix_timestamp(lolt1.in_warehouse_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(ogs.confirm_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 / 24 < 7 then ogs.order_goods_id else null end end) is_7_day_in_warehouse, --7天入库
  count(distinct case when lolt2.out_warehouse_time is null or unix_timestamp(lolt2.out_warehouse_time,'yyyy-MM-dd HH:mm:ss') < unix_timestamp(ogs.confirm_time,'yyyy-MM-dd HH:mm:ss')  then null else case when (unix_timestamp(lolt2.out_warehouse_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(ogs.confirm_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 / 24 < 9 then ogs.order_goods_id else null end end) is_9_day_out_warehouse, --9天出库
  count(distinct case when rr.create_time is null  or unix_timestamp(rr.create_time,'yyyy-MM-dd HH:mm:ss') < unix_timestamp(ogs.confirm_time,'yyyy-MM-dd HH:mm:ss')  then null else case when (unix_timestamp(rr.create_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(ogs.confirm_time,'yyyy-MM-dd HH:mm:ss')) / 60 / 60 / 24 > 12 and rr.order_goods_id is not null then rr.order_goods_id else null end end) is_12_day_refund --12天未入库取消

FROM ods_vova_vts.ods_vova_order_goods_status ogs
  INNER JOIN ods_vova_vts.ods_vova_order_goods og ON ogs.order_goods_id = og.rec_id
  INNER JOIN ods_vova_vts.ods_vova_order_info oi ON oi.order_id = og.order_id
  INNER JOIN ods_vova_vts.ods_vova_region r ON r.region_id = oi.country
  INNER JOIN ods_vova_vts.ods_vova_goods g ON g.goods_id = og.goods_id
  LEFT JOIN ods_vova_vts.ods_vova_refund_reason rr ON rr.order_goods_id = og.rec_id
     INNER JOIN ods_vova_vts.ods_vova_order_goods_extra oge ON ogs.order_goods_id = oge.order_goods_id
  LEFT JOIN ods_vova_vts.ods_vova_refund_audit_txn rat ON rr.final_txn_id = rat.txn_id
  join ods.vovapost_combine_order_goods cog on og.order_goods_sn = cog.member_id
  left join ods.vovapost_logistics_order_label_tracking lolt1 on cog.first_mile_tracking_number = lolt1.logistics_tracking_number
  left join ods.vovapost_logistics_order_label_tracking lolt2 on cog.last_mile_tracking_number = lolt2.logistics_tracking_number
WHERE to_date(ogs.confirm_time) >= '${cur_date}'  and to_date(ogs.confirm_time) < to_date(now())
      AND oi.email NOT REGEXP '@tetx.com|@i9i8.com'
      AND oi.pay_status >= 1
      and oge.collection_plan_id = 2
group by cube (nvl(r.region_code,'NA'),if(cog.logistics_product_id = 2,'自寄','物流商揽收'),to_date(ogs.confirm_time))
) a
left join (
SELECT
  nvl(nvl(r.region_code,'NA'),'all') region_code,nvl(to_date(ogs.confirm_time),'all') pt,
  count(distinct ogs.order_goods_id) logistics_cnt --总运单数
FROM ods_vova_vts.ods_vova_order_goods_status ogs
  INNER JOIN ods_vova_vts.ods_vova_order_goods og ON ogs.order_goods_id = og.rec_id
  INNER JOIN ods_vova_vts.ods_vova_order_info oi ON oi.order_id = og.order_id
  INNER JOIN ods_vova_vts.ods_vova_region r ON r.region_id = oi.country
WHERE to_date(ogs.confirm_time) >= '${cur_date}' and to_date(ogs.confirm_time) < to_date(now())
      AND oi.email NOT REGEXP '@tetx.com|@i9i8.com'
      AND oi.pay_status >= 1
group by cube (nvl(r.region_code,'NA'),to_date(ogs.confirm_time))
) b
on a.region_code = b.region_code and a.pt = b.pt
where a.pt != 'all' and b.pt != 'all';
"


#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi




