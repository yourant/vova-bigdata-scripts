#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "cur_date:'${cur_date}'"

sql="
insert overwrite table dwb.dwb_vova_refund_monitor_service PARTITION (pt = '${cur_date}')
select
'${cur_date}' as cur_date,
nvl(dog.region_code,'all') as region_code,
nvl(fr.refund_reason,'all') as refund_reason,
count(distinct if(to_date(fr.create_time) = '${cur_date}' and fr.refund_type_id = 2,fr.order_goods_id,null)) as refund_order_cnt,
count(distinct if(to_date(fr.audit_time) = '${cur_date}' and fr.refund_type_id = 2 and (rr.audit_note_cn like '%case: 10%' or rr.audit_note_cn like '%case: 13%' or rr.audit_note_cn like '%case: 16%' or rr.audit_note_cn like '%case: 17%'
or rr.audit_note_cn like '%case: 18%' or rr.audit_note_cn like '%case: 19%' or rr.audit_note_cn like '%case: 20%' or rr.audit_note_cn like '%case:2%' or rr.audit_note_cn like '%case:3%' or
rr.audit_note_cn like '%case:4%' or rr.audit_note_cn like '%case:5%' or rr.audit_note_cn like '%case:15%') ,fr.order_goods_id,null)) as system_audit_cnt,
count(distinct if(to_date(fr.audit_time) = '${cur_date}' and fr.refund_type_id = 2 and (fr.audit_status = 'mct_audit_rejected' or fr.audit_status = 'mct_audit_passed'), fr.order_goods_id,null)) as mct_audit_cnt,
count(distinct if(to_date(fr.audit_time) = '${cur_date}' and fr.refund_type_id = 2 and (fr.audit_status = 'audit_rejected' or fr.audit_status = 'audit_passed'), fr.order_goods_id,null)) as service_audit_cnt,
count(distinct if(to_date(fr.audit_time) = '${cur_date}' and fr.refund_type_id = 2 and fr.audit_status = 'audit_passed', fr.order_goods_id,null)) as service_audit_passed_cnt,
nvl(count(distinct if(to_date(fr.audit_time) = '${cur_date}' and fr.refund_type_id = 2 and fr.audit_status = 'audit_passed', fr.order_goods_id,null))/
count(distinct if(to_date(fr.audit_time) = '${cur_date}' and fr.refund_type_id = 2 and (fr.audit_status = 'audit_rejected' or fr.audit_status = 'audit_passed'), fr.order_goods_id,null)),0)*100 as service_audit_passed_rate,
count(distinct if(fr.refund_type_id = 2 and fr.audit_status = 'to_audit', fr.order_goods_id,null)) as to_audit_cnt,
count(distinct if(fr.refund_type_id = 2 and fr.audit_status = 'to_audit' and (unix_timestamp('${cur_date}','yyyy-MM-dd')-unix_timestamp(fr.audit_time,'yyyy-MM-dd HH:mm:ss'))/3600<=24 and (unix_timestamp('${cur_date}','yyyy-MM-dd')-unix_timestamp(fr.audit_time,'yyyy-MM-dd HH:mm:ss'))/3600>=0, fr.order_goods_id,null)) as to_audit_cnt_24,
nvl(count(distinct if(fr.refund_type_id = 2 and fr.audit_status = 'to_audit' and (unix_timestamp('${cur_date}','yyyy-MM-dd')-unix_timestamp(fr.audit_time,'yyyy-MM-dd HH:mm:ss'))/3600<=24 and (unix_timestamp('${cur_date}','yyyy-MM-dd')-unix_timestamp(fr.audit_time,'yyyy-MM-dd HH:mm:ss'))/3600>=0, fr.order_goods_id,null))/
count(distinct if(fr.refund_type_id = 2 and fr.audit_status = 'to_audit', fr.order_goods_id,null)),0)*100 as to_audit_24_rate,
count(distinct if(fr.refund_type_id = 2 and fr.audit_status = 'to_audit' and (unix_timestamp('${cur_date}','yyyy-MM-dd')-unix_timestamp(fr.audit_time,'yyyy-MM-dd HH:mm:ss'))/3600<=48 and (unix_timestamp('${cur_date}','yyyy-MM-dd')-unix_timestamp(fr.audit_time,'yyyy-MM-dd HH:mm:ss'))/3600>=0, fr.order_goods_id,null)) as to_audit_cnt_48,
nvl(count(distinct if(fr.refund_type_id = 2 and fr.audit_status = 'to_audit' and (unix_timestamp('${cur_date}','yyyy-MM-dd')-unix_timestamp(fr.audit_time,'yyyy-MM-dd HH:mm:ss'))/3600<=48 and (unix_timestamp('${cur_date}','yyyy-MM-dd')-unix_timestamp(fr.audit_time,'yyyy-MM-dd HH:mm:ss'))/3600>=0, fr.order_goods_id,null))/
count(distinct if(fr.refund_type_id = 2 and fr.audit_status = 'to_audit', fr.order_goods_id,null)),0)*100 as to_audit_48_rate
from dwd.dwd_vova_fact_refund fr
left join dim.dim_vova_order_goods dog
on dog.order_goods_id = fr.order_goods_id
left join ods_vova_vts.ods_vova_refund_reason rr
on rr.order_goods_id = fr.order_goods_id
where datediff('${cur_date}',to_date(fr.create_time)) < 180
and datediff('${cur_date}',to_date(fr.create_time)) >= 0
and fr.refund_reason is not null and fr.refund_reason != 'NULL'
and dog.region_code is not null
group by
'${cur_date}',
dog.region_code,
fr.refund_reason
grouping sets(
('${cur_date}'),
('${cur_date}',dog.region_code),
('${cur_date}',fr.refund_reason),
('${cur_date}',dog.region_code,fr.refund_reason)
)
;
"

spark-sql \
--conf "spark.app.name=dwb_vova_refund_monitor_service" \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

if [ $? -ne 0 ]; then
  exit 1
fi