#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-43 day" +%Y-%m-%d`
fi

spark-sql  --conf "spark.app.name=dwb_vova_refund_monitor"  --conf "spark.dynamicAllocation.maxExecutors=100"  -e "

drop table if exists tmp.tmp_4274_orde_cnt;
create table tmp.tmp_4274_orde_cnt as
select
nvl(og.region_code,'all') region_code,
count(*) orde_cnt, --确认订单数
count(if(og.sku_shipping_status in (1,2),og.order_goods_id,null)) mark_suces_orde_cnt, --标记发货订单数
count(if(og.sku_shipping_status in (1,2) and fr.order_goods_id is null,og.order_goods_id,null)) suces_orde_cnt, --成功发货订单数
sum(if(og.sku_shipping_status in (1,2) and fr.order_goods_id is null,(dg.shop_price + dg.shipping_fee),null)) suces_orde_money, --成功发货订单额
concat(round(count(if(og.sku_shipping_status in (1,2) and fr.order_goods_id is null,og.order_goods_id,null)) * 100 / count(*),2),'%') actual_order_rate --实际发货率
from dim.dim_vova_order_goods og
join dim.dim_vova_goods dg
on og.goods_id = dg.goods_id
left join (select * from dwd.dwd_vova_fact_refund where refund_type_id in (3,4,8,9,10,11,13,14)) fr on og.order_goods_id=fr.order_goods_id
where to_date(og.order_time) = '${cur_date}'
group by cube (og.region_code)
;

drop table if exists tmp.tmp_4274_refund_orde_cnt;
create table tmp.tmp_4274_refund_orde_cnt as
select
nvl(og.region_code,'all') region_code,
nvl(fr.refund_reason,'all') refund_reason,
count(distinct fr.order_goods_id)  refund_cnt, --退款申请次数
count(distinct if(fr.sku_pay_status in (3,4) and fr.refund_type_id = 2,fr.order_goods_id,null)) refund_pass_cnt, --退款通过次数
count(distinct if(fr.sku_pay_status in (3,4) and fr.refund_type_id = 2,if(vrat.order_goods_id = null, fr.order_goods_id, vrat_1.order_goods_id),null)) refund_cnt_1, --1次通过
count(distinct if(fr.sku_pay_status in (3,4) and fr.refund_type_id = 2,vrat_2.order_goods_id,null)) refund_cnt_2, --2次通过
count(distinct if(fr.sku_pay_status in (3,4) and fr.refund_type_id = 2,vrat_3.order_goods_id,null)) refund_cnt_3, --3次通过
count(distinct if(fr.sku_pay_status in (3,4) and fr.refund_type_id = 2,vrat_4.order_goods_id,null)) refund_cnt_4, --4次通过
count(distinct if(vrat.recheck_type = 2,fr.order_goods_id,null)) appeal_rate, --申诉次数
count(distinct if(vrat.audit_status = 'audit_passed' and vrat.recheck_type = 2,fr.order_goods_id,null)) appeal_pass_rate --申诉成功次数
from dwd.dwd_vova_fact_refund fr
join dim.dim_vova_order_goods og
on fr.order_goods_id=og.order_goods_id
left join ods_vova_vts.ods_vova_refund_audit_txn vrat
on fr.order_goods_id=vrat.order_goods_id
left join (select order_goods_id,count(*) cnt from ods_vova_vts.ods_vova_refund_audit_txn group by order_goods_id having cnt = 1) vrat_1
on fr.order_goods_id=vrat_1.order_goods_id
left join (select order_goods_id,count(*) cnt from ods_vova_vts.ods_vova_refund_audit_txn group by order_goods_id having cnt = 2) vrat_2
on fr.order_goods_id=vrat_2.order_goods_id
left join (select order_goods_id,count(*) cnt from ods_vova_vts.ods_vova_refund_audit_txn group by order_goods_id having cnt = 3) vrat_3
on fr.order_goods_id=vrat_3.order_goods_id
left join (select order_goods_id,count(*) cnt from ods_vova_vts.ods_vova_refund_audit_txn group by order_goods_id having cnt > 3) vrat_4
on fr.order_goods_id=vrat_4.order_goods_id
where to_date(fr.create_time) = '${cur_date}'
and fr.refund_reason is not null and fr.refund_reason != 'NULL'
group by cube (og.region_code,fr.refund_reason)
;

insert overwrite table dwb.dwb_vova_refund_monitor  PARTITION (pt = '${cur_date}')
select
'${cur_date}' cur_date,
a.region_code,
b.refund_reason,
a.orde_cnt, --确认订单数
nvl(a.mark_suces_orde_cnt,0) mark_suces_orde_cnt, --标记发货订单数
nvl(a.suces_orde_cnt,0) suces_orde_cnt, --成功发货订单数
nvl(a.suces_orde_money,0) suces_orde_money, --成功发货订单额
nvl(a.actual_order_rate,0) actual_order_rate, --实际发货率
nvl(b.refund_cnt,0) refund_cnt, --退款申请次数
nvl(b.refund_pass_cnt,0) refund_pass_cnt, --退款通过次数
b.refund_cnt_1, --1次通过
b.refund_cnt_2, --2次通过
b.refund_cnt_3, --3次通过
b.refund_cnt_4, --4次通过
b.appeal_rate, --申诉次数
b.appeal_pass_rate --申诉通过次数
from tmp.tmp_4274_orde_cnt a
left join tmp.tmp_4274_refund_orde_cnt b
on a.region_code = b.region_code
;
"

cur_date=`date -d " 63 day ago ${cur_date}" +%Y%m%d`
spark-sql  --conf "spark.app.name=refund_monitor_63d"  --conf "spark.dynamicAllocation.maxExecutors=100"  -e "
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=1000;

insert overwrite table dwb.dwb_vova_refund_week_rate  PARTITION (pt)
select * from (
select
'${cur_date}' cur_date,
nvl(tmp.region_code,'all') region_code,
nvl(tmp.refund_reason,'all') refund_reason,
concat(round(count(distinct refund_4_id) * 100 / count(distinct order_goods_id),2),'%')  refund_4rate, --4周退款率
concat(round(count(distinct refund_6_id) * 100 / count(distinct order_goods_id),2),'%')  refund_6rate, --6周退款率
concat(round(count(distinct refund_9_id) * 100 / count(distinct order_goods_id),2),'%')  refund_9rate, --9周退款率
concat(round(count(distinct refund_12_id) * 100 / count(distinct order_goods_id),2),'%')  refund_12rate, --12周退款率
concat(round(count(distinct refund_15_id) * 100 / count(distinct order_goods_id),2),'%')  refund_15rate, --15周退款率
nvl(tmp.confirm_date,'all') pt
from
(select
nvl(og.region_code,'NA') region_code,
nvl(fr.refund_reason,'NA') refund_reason,
to_date(og.confirm_time) confirm_date,
if(to_date(fr.create_time) <= date_add('${cur_date}',29) and fr.refund_type_id = 2,fr.order_goods_id,null) refund_4_id,
if(to_date(fr.create_time) <= date_add('${cur_date}',43) and fr.refund_type_id = 2,fr.order_goods_id,null) refund_6_id,
if(to_date(fr.create_time) <= date_add('${cur_date}',64) and fr.refund_type_id = 2,fr.order_goods_id,null) refund_9_id,
if(to_date(fr.create_time) <= date_add('${cur_date}',85) and fr.refund_type_id = 2,fr.order_goods_id,null) refund_12_id,
if(to_date(fr.create_time) <= date_add('${cur_date}',106) and fr.refund_type_id = 2,fr.order_goods_id,null) refund_15_id,
og.order_goods_id
from dim.dim_vova_order_goods og
left join dwd.dwd_vova_fact_refund fr on og.order_goods_id=fr.order_goods_id
where to_date(og.confirm_time) >= '${cur_date}' and to_date(og.confirm_time) <= date_add('${cur_date}',63)
) tmp
group by cube (tmp.region_code,tmp.refund_reason,tmp.confirm_date)
) where pt != 'all';
;
"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
