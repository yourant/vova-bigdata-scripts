#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "cur_date:'${cur_date}'"

sql="
drop table if exists tmp.tmp_4274_orde_cnt_ysj;
create table tmp.tmp_4274_orde_cnt_ysj as
select
to_date(og.order_time) as order_time,
nvl(og.region_code,'all') region_code,
count(*) orde_cnt, --确认订单数
count(if(og.sku_shipping_status in (1,2),og.order_goods_id,null)) mark_suces_orde_cnt, --标记发货订单数
count(if(og.sku_shipping_status in (1,2) and fr.order_goods_id is null,og.order_goods_id,null)) suces_orde_cnt, --成功发货订单数
sum(if(og.sku_shipping_status in (1,2) and fr.order_goods_id is null,(dg.shop_price + dg.shipping_fee),null)) suces_orde_money --成功发货订单额
from dim.dim_vova_order_goods og
join dim.dim_vova_goods dg
on og.goods_id = dg.goods_id
left join (select * from dwd.dwd_vova_fact_refund where refund_type_id in (3,4,8,9,10,11,13,14)) fr on og.order_goods_id=fr.order_goods_id
where datediff('${cur_date}',to_date(og.order_time)) >= 58
and datediff('${cur_date}',to_date(og.order_time)) < 88
group by
to_date(og.order_time),
og.region_code
grouping sets (
(to_date(og.order_time)),
(to_date(og.order_time),og.region_code)
)
;

drop table if exists tmp.tmp_4274_refund_orde_cnt_ysj;
create table tmp.tmp_4274_refund_orde_cnt_ysj as
select
to_date(fr.create_time) as create_time,
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
where datediff('${cur_date}',to_date(fr.create_time)) >= 58
and datediff('${cur_date}',to_date(fr.create_time)) < 88
and fr.refund_reason is not null and fr.refund_reason != 'NULL'
group by to_date(fr.create_time),og.region_code,fr.refund_reason
grouping sets(
(to_date(fr.create_time)),
(to_date(fr.create_time),og.region_code),
(to_date(fr.create_time),fr.refund_reason),
(to_date(fr.create_time),og.region_code,fr.refund_reason)
)
;

insert overwrite table dwb.dwb_vova_refund_monitor_v2 PARTITION (pt = '${cur_date}')
select
a.order_time cur_date,
a.region_code,
b.refund_reason,
a.orde_cnt, --确认订单数
nvl(a.mark_suces_orde_cnt,0) mark_suces_orde_cnt, --标记发货订单数
nvl(a.suces_orde_cnt,0) suces_orde_cnt, --成功发货订单数
nvl(a.suces_orde_money,0) suces_orde_money, --成功发货订单额
nvl(a.suces_orde_cnt/a.orde_cnt,0)*100 actual_order_rate, --实际发货率
nvl(b.refund_cnt,0) refund_cnt, --退款申请次数
nvl(b.refund_pass_cnt,0) refund_pass_cnt, --退款通过次数
nvl(b.refund_cnt_1/b.refund_cnt,0)*100 as refund_cnt_1, --1次通过
nvl(b.refund_cnt_2/b.refund_cnt,0)*100 as refund_cnt_2, --2次通过
nvl(b.refund_cnt_3/b.refund_cnt,0)*100 as refund_cnt_3, --3次通过
nvl(b.refund_cnt_4/b.refund_cnt,0)*100 as refund_cnt_4, --4次通过
nvl(b.appeal_rate/a.suces_orde_cnt,0)*100 as appeal_rate, --申诉次数
nvl(b.appeal_pass_rate/b.appeal_rate,0)*100 as appeal_pass_rate --申诉通过次数
from tmp.tmp_4274_orde_cnt_ysj a
left join tmp.tmp_4274_refund_orde_cnt_ysj b
on a.region_code = b.region_code
and a.order_time = b.create_time
;
"

spark-sql \
--conf "spark.app.name=dwb_vova_refund_monitor_v2" \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

if [ $? -ne 0 ]; then
  exit 1
fi