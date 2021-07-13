#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "cur_date:'${cur_date}'"

sql="
drop table if exists tmp.tmp_refund_monitor_mct_mct;
create table tmp.tmp_refund_monitor_mct_mct as
select
to_date(og.order_time) as order_time,
nvl(og.region_code,'all') region_code,
nvl(og.mct_id,'all') mct_id,
count(distinct og.mct_id) as mct_cnt,
count(distinct og.order_goods_id) as suces_orde_cnt
from dim.dim_vova_order_goods og
inner join (
select distinct order_goods_id
from dwd.dwd_vova_fact_logistics
where year(to_date(valid_tracking_date)) > 1971) fr on og.order_goods_id=fr.order_goods_id
where datediff('${cur_date}',to_date(og.order_time)) >= 58
and datediff('${cur_date}',to_date(og.order_time)) < 88
group by
to_date(og.order_time),
og.region_code,
og.mct_id
grouping sets (
(to_date(og.order_time)),
(to_date(og.order_time),og.region_code),
(to_date(og.order_time),og.mct_id),
(to_date(og.order_time),og.region_code,og.mct_id)
)
;

drop table if exists tmp.tmp_refund_monitor_mct_refund;
create table tmp.tmp_refund_monitor_mct_refund as
select
to_date(fr.create_time) as create_time,
nvl(dog.region_code,'all') as region_code,
nvl(fr.refund_reason,'all') as refund_reason,
count(distinct if(fr.refund_type_id = 2 and (vrat.audit_status = 'mct_audit_rejected' or vrat.audit_status = 'mct_audit_passed'), fr.order_goods_id,null)) as mct_audit_cnt,
count(distinct if(fr.refund_type_id = 2 and vrat.audit_status = 'mct_audit_passed', fr.order_goods_id,null)) as mct_audit_passed_cnt,
nvl(count(distinct if(fr.refund_type_id = 2 and vrat.audit_status = 'mct_audit_passed', fr.order_goods_id,null))/
count(distinct if(fr.refund_type_id = 2 and (vrat.audit_status = 'mct_audit_rejected' or vrat.audit_status = 'mct_audit_passed'), fr.order_goods_id,null)),0)*100 as mct_audit_passed_rate,
count(distinct if(fr.refund_type_id = 2 and vrat.audit_status = 'mct_audit_rejected', fr.order_goods_id,null)) as mct_audit_rejected_cnt
from dwd.dwd_vova_fact_refund fr
left join dim.dim_vova_order_goods dog
on dog.order_goods_id = fr.order_goods_id
left join ods_vova_vts.ods_vova_refund_audit_txn vrat
on fr.order_goods_id=vrat.order_goods_id
where datediff('${cur_date}',to_date(fr.create_time)) >= 58
and datediff('${cur_date}',to_date(fr.create_time)) < 88
and fr.refund_reason is not null and fr.refund_reason != 'NULL'
and dog.region_code is not null
group by to_date(fr.create_time),dog.region_code,fr.refund_reason
grouping sets(
(to_date(fr.create_time)),
(to_date(fr.create_time),dog.region_code),
(to_date(fr.create_time),fr.refund_reason),
(to_date(fr.create_time),dog.region_code,fr.refund_reason)
)
;

drop table if exists tmp.tmp_refund_monitor_mct_appeal;
create table tmp.tmp_refund_monitor_mct_appeal as
select
to_date(fr.create_time) as create_time,
nvl(dog.region_code,'all') as region_code,
nvl(fr.refund_reason,'all') as refund_reason,
nvl(dog.mct_id,'all') mct_id,
count(distinct vrat.order_goods_id) as appeal_cnt
from dwd.dwd_vova_fact_refund fr
left join dim.dim_vova_order_goods dog
on dog.order_goods_id = fr.order_goods_id
left join ods_vova_vts.ods_vova_refund_audit_txn vrat
on fr.order_goods_id=vrat.order_goods_id
where vrat.order_goods_id in (
select distinct order_goods_id
from ods_vova_vts.ods_vova_refund_audit_txn
where (audit_status = 'mct_audit_rejected'
or audit_status = 'mct_audit_passed')
and refund_type_id = 2
)
and vrat.recheck_type = 2
and datediff('${cur_date}',to_date(fr.create_time)) >= 58
and datediff('${cur_date}',to_date(fr.create_time)) < 88
group by
to_date(fr.create_time),
dog.region_code,
fr.refund_reason,
dog.mct_id
grouping sets(
(to_date(fr.create_time)),
(to_date(fr.create_time),dog.region_code),
(to_date(fr.create_time),fr.refund_reason),
(to_date(fr.create_time),dog.mct_id),
(to_date(fr.create_time),dog.region_code,fr.refund_reason),
(to_date(fr.create_time),dog.region_code,dog.mct_id),
(to_date(fr.create_time),fr.refund_reason,dog.mct_id),
(to_date(fr.create_time),dog.region_code,fr.refund_reason,dog.mct_id)
)
;

insert overwrite table dwb.dwb_vova_refund_monitor_mct PARTITION (pt = '${cur_date}')
select *
from (
select
nvl(order_time,b.create_time) as cur_date,
nvl(a.region_code,b.region_code) as region_code,
b.refund_reason,
nvl(mct_cnt,0) as mct_cnt,
nvl(mct_audit_cnt,0) as mct_audit_cnt,
nvl(mct_audit_passed_cnt,0) as mct_audit_passed_cnt,
nvl(mct_audit_passed_rate,0) as mct_audit_passed_rate,
nvl(mct_audit_rejected_cnt,0) as mct_audit_rejected_cnt,
nvl(appeal_cnt/suces_orde_cnt,0)*100 as appeal_cnt,
nvl(mct_early_warning_cnt,0) as mct_early_warning_cnt,
nvl(mct_overproof_cnt,0) as mct_overproof_cnt
from tmp.tmp_refund_monitor_mct_mct a
full outer join tmp.tmp_refund_monitor_mct_refund b
on a.region_code = b.region_code
and a.order_time = b.create_time
and a.mct_id = 'all'
left join tmp.tmp_refund_monitor_mct_appeal c
on c.create_time = b.create_time
and c.region_code = b.region_code
and c.refund_reason = b.refund_reason
and c.mct_id = 'all'
left join (select
create_time,
region_code,
refund_reason,
count(distinct mct_id) as mct_early_warning_cnt
from (
select
a.*,
nvl(a.appeal_cnt/suces_orde_cnt,0)*100 as appeal_rate
from tmp.tmp_refund_monitor_mct_appeal a
left join tmp.tmp_refund_monitor_mct_mct b
on a.create_time =b.order_time
and a.region_code = b.region_code
and a.mct_id = b.mct_id
) tmp
where appeal_rate >= 1
and appeal_rate < 5
group by
create_time,
region_code,
refund_reason
) d
on d.create_time = b.create_time
and d.region_code = b.region_code
and d.refund_reason = b.refund_reason
left join (select
create_time,
region_code,
refund_reason,
count(distinct mct_id) as mct_overproof_cnt
from (
select
a.*,
nvl(a.appeal_cnt/suces_orde_cnt,0)*100 as appeal_rate
from tmp.tmp_refund_monitor_mct_appeal a
left join tmp.tmp_refund_monitor_mct_mct b
on a.create_time =b.order_time
and a.region_code = b.region_code
and a.mct_id = b.mct_id
) tmp
where appeal_rate >=5
group by
create_time,
region_code,
refund_reason
) e
on e.create_time = b.create_time
and e.region_code = b.region_code
and e.refund_reason = b.refund_reason
) tmp
where refund_reason is not null
;
"

spark-sql \
--conf "spark.app.name=dwb_vova_refund_monitor_mct" \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

if [ $? -ne 0 ]; then
  exit 1
fi