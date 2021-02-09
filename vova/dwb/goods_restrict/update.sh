#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
fi
#drop table if exists tmp.rpt_goods_restrict_cat_id;
#create table
#drop table if exists tmp.rpt_goods_restrict_gs_id;
#create table
sql="
with tmp_rpt_goods_restrict_cat_id as
(
select
t1.first_cat_id,
sum(t1.nlrf_order_cnt_5_8w)/count(t1.order_goods_id) as nlrf_rate_5_8w
from
(
select
c.first_cat_id,
og.order_goods_id,
og.goods_number,
case when datediff(fr.audit_time,og.confirm_time)<63 and  fr.refund_reason_type_id not in (8,9) and fr.refund_type_id=2 and fr.rr_audit_status='audit_passed' and og.sku_pay_status>1 then 1 else 0 end nlrf_order_cnt_5_8w
from dim.dim_vova_order_goods og
left join dwd.dwd_vova_fact_refund fr on fr.order_goods_id=og.order_goods_id
left join dwd.dwd_vova_fact_logistics fl on fr.order_goods_id=fl.order_goods_id
left join dim.dim_vova_category c on og.cat_id = c.cat_id
where datediff('${pt}', date(og.confirm_time)) between 62 and 92
) t1
group by t1.first_cat_id
),
tmp_rpt_goods_restrict_gs_id as (
select
t1.goods_id,
t1.first_cat_id,
sum(t1.nlrf_order_cnt_5_8w)/count(t1.order_goods_id) as nlrf_rate_5_8w,
sum(t1.goods_number) sales_order
from
(
select
og.goods_id,
c.first_cat_id,
og.order_goods_id,
og.goods_number,
case when datediff(fr.audit_time,og.confirm_time)<63 and  fr.refund_reason_type_id not in (8,9) and fr.refund_type_id=2 and fr.rr_audit_status='audit_passed' and og.sku_pay_status>1 then 1 else 0 end nlrf_order_cnt_5_8w
from dim.dim_vova_order_goods og
left join dwd.dwd_vova_fact_refund fr on fr.order_goods_id=og.order_goods_id
left join dwd.dwd_vova_fact_logistics fl on fr.order_goods_id=fl.order_goods_id
left join dim.dim_vova_category c on og.cat_id = c.cat_id
where datediff('${pt}', date(og.confirm_time)) between 62 and 92
) t1 group by t1.goods_id,t1.first_cat_id
)
insert overwrite table dwb.dwb_vova_goods_restrict_d PARTITION (pt = '${pt}')
select
/*+ REPARTITION(1) */
'$pt' event_date,
g.goods_id,
dg.first_cat_name,
g.nlrf_rate_5_8w/c.nlrf_rate_5_8w times,
nvl(t.impressions,0) impressions
from tmp_rpt_goods_restrict_gs_id g
left join tmp_rpt_goods_restrict_cat_id c on g.first_cat_id = c.first_cat_id
left join dim.dim_vova_goods dg on g.goods_id = dg.goods_id
left join (select virtual_goods_id,count(*) impressions from dwd.dwd_vova_log_goods_impression where pt='$pt' and dp = 'vova' and platform='mob' and virtual_goods_id>0 group by virtual_goods_id) t on t.virtual_goods_id = dg.virtual_goods_id
where g.nlrf_rate_5_8w>0 and g.sales_order >10 and g.nlrf_rate_5_8w/c.nlrf_rate_5_8w>3;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql --conf "spark.app.name=dwb_vova_goods_restrict_d_zhangyin" --conf "spark.dynamicAllocation.maxExecutors=100" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi