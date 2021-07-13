#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "cur_date:'${cur_date}'"

sql="
insert overwrite table dwb.dwb_vova_refund_monitor_system PARTITION (pt = '${cur_date}')
select
to_date(fr.create_time) as create_time,
nvl(dog.region_code,'all') as region_code,
nvl(fr.refund_reason,'all') as refund_reason,
count(distinct if(fr.refund_type_id = 2 and (rr.audit_note_cn like '%case: 10%' or rr.audit_note_cn like '%case: 13%' or rr.audit_note_cn like '%case: 16%' or rr.audit_note_cn like '%case: 17%'
or rr.audit_note_cn like '%case: 18%' or rr.audit_note_cn like '%case: 19%' or rr.audit_note_cn like '%case: 20%' or rr.audit_note_cn like '%case:2%' or rr.audit_note_cn like '%case:3%' or
rr.audit_note_cn like '%case:4%' or rr.audit_note_cn like '%case:5%' or rr.audit_note_cn like '%case:15%') ,fr.order_goods_id,null)) as system_audit_cnt,
count(distinct if(fr.refund_type_id = 2 and vrat.audit_status = 'audit_passed' and (rr.audit_note_cn like '%case: 10%' or rr.audit_note_cn like '%case: 13%' or rr.audit_note_cn like '%case: 16%' or rr.audit_note_cn like '%case: 17%'
or rr.audit_note_cn like '%case: 18%' or rr.audit_note_cn like '%case: 19%' or rr.audit_note_cn like '%case: 20%' ),fr.order_goods_id,null)) as system_audit_passed_cnt,
nvl(count(distinct if(fr.refund_type_id = 2 and vrat.audit_status = 'audit_passed' and (rr.audit_note_cn like '%case: 10%' or rr.audit_note_cn like '%case: 13%' or rr.audit_note_cn like '%case: 16%' or rr.audit_note_cn like '%case: 17%'
or rr.audit_note_cn like '%case: 18%' or rr.audit_note_cn like '%case: 19%' or rr.audit_note_cn like '%case: 20%' ),fr.order_goods_id,null)) / count(distinct fr.order_goods_id),0)*100 as system_audit_passed_rate,
nvl(count(distinct if(fr.refund_type_id = 2 and vrat.recheck_type = 2 and (rr.audit_note_cn like '%case:2%' or rr.audit_note_cn like '%case:3%' or rr.audit_note_cn like '%case:4%' or rr.audit_note_cn like '%case:5%' or rr.audit_note_cn like '%case:15%'),fr.order_goods_id,null))/
count(distinct if(fr.refund_type_id = 2 and (rr.audit_note_cn like '%case: 10%' or rr.audit_note_cn like '%case: 13%' or rr.audit_note_cn like '%case: 16%' or rr.audit_note_cn like '%case: 17%'
or rr.audit_note_cn like '%case: 18%' or rr.audit_note_cn like '%case: 19%' or rr.audit_note_cn like '%case: 20%' or rr.audit_note_cn like '%case:2%' or rr.audit_note_cn like '%case:3%' or
rr.audit_note_cn like '%case:4%' or rr.audit_note_cn like '%case:5%' or rr.audit_note_cn like '%case:15%') ,fr.order_goods_id,null)),0)*100 as system_appeal_rate,
count(distinct if(fr.refund_type_id = 2 and rr.audit_note_cn LIKE '%case:2%',fr.order_goods_id,null)) as case_2,
count(distinct if(fr.refund_type_id = 2 and rr.audit_note_cn LIKE '%case:3%',fr.order_goods_id,null)) as case_3,
count(distinct if(fr.refund_type_id = 2 and rr.audit_note_cn LIKE '%case:4%',fr.order_goods_id,null)) as case_4,
count(distinct if(fr.refund_type_id = 2 and rr.audit_note_cn LIKE '%case:5%',fr.order_goods_id,null)) as case_5,
count(distinct if(fr.refund_type_id = 2 and rr.audit_note_cn LIKE '%case: 10%',fr.order_goods_id,null)) as case_10,
count(distinct if(fr.refund_type_id = 2 and rr.audit_note_cn LIKE '%case: 13%',fr.order_goods_id,null)) as case_13,
count(distinct if(fr.refund_type_id = 2 and rr.audit_note_cn LIKE '%case: 16%',fr.order_goods_id,null)) as case_16,
count(distinct if(fr.refund_type_id = 2 and rr.audit_note_cn LIKE '%case: 17%',fr.order_goods_id,null)) as case_17,
count(distinct if(fr.refund_type_id = 2 and rr.audit_note_cn LIKE '%case: 18%',fr.order_goods_id,null)) as case_18,
count(distinct if(fr.refund_type_id = 2 and rr.audit_note_cn LIKE '%case: 19%',fr.order_goods_id,null)) as case_19,
count(distinct if(fr.refund_type_id = 2 and rr.audit_note_cn LIKE '%case: 20%',fr.order_goods_id,null)) as case_20,
count(distinct if(fr.refund_type_id = 2 and rr.audit_note_cn LIKE '%case:21%',fr.order_goods_id,null)) as case_21,
count(distinct if(fr.refund_type_id = 2 and rr.audit_note_cn LIKE '%case:22%',fr.order_goods_id,null)) as case_22,
count(distinct if(fr.refund_type_id = 2 and rr.audit_note_cn LIKE '%case:23%',fr.order_goods_id,null)) as case_23,
count(distinct if(fr.refund_type_id = 2 and rr.audit_note_cn LIKE '%case:24%',fr.order_goods_id,null)) as case_24,
count(distinct if(fr.refund_type_id = 2 and rr.audit_note_cn LIKE '%case:25%',fr.order_goods_id,null)) as case_25
from dwd.dwd_vova_fact_refund fr
left join ods_vova_vts.ods_vova_refund_reason rr
on rr.order_goods_id = fr.order_goods_id
left join ods_vova_vts.ods_vova_refund_audit_txn vrat
on fr.order_goods_id=vrat.order_goods_id
left join dim.dim_vova_order_goods dog
on dog.order_goods_id = fr.order_goods_id
where to_date(fr.create_time) = '${cur_date}'
and fr.refund_reason is not null and fr.refund_reason != 'NULL'
and dog.region_code is not null
group by
to_date(fr.create_time),
dog.region_code,
fr.refund_reason
grouping sets(
(to_date(fr.create_time)),
(to_date(fr.create_time),dog.region_code),
(to_date(fr.create_time),fr.refund_reason),
(to_date(fr.create_time),dog.region_code,fr.refund_reason)
)
;
"

spark-sql \
--conf "spark.app.name=dwb_vova_refund_monitor_system" \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

if [ $? -ne 0 ]; then
  exit 1
fi