#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "0 day" +%Y-%m-%d`
pre_2w=`date -d "15 day ago ${pt}" +%Y-%m-%d`
fi
sql="
with ads_gsn_reduce_valid_goods as
(
select
goods_id,
add_cycle,
to_date(start_time) start_time,
to_date(end_time) end_time
from ods_vova_vts.ods_vova_gsn_reduce_valid_goods
where to_date(start_time)<='$pt' and to_date(end_time)>='$pt'
)
insert overwrite table ads.ads_vova_gsn_reduce_valid_goods PARTITION (pt = '$pt')
select
/*+ REPARTITION(1) */
g.goods_id,
g.add_cycle,
nvl(t1.expre,0) expre,
nvl(t2.sales_order,0) sales_order,
nvl(t2.payed_uv/t1.expre_uv,0) expre_cr
from ads_gsn_reduce_valid_goods g
left join
(
select
t.goods_id,
t.add_cycle,
count(*) expre,
count(distinct gi.device_id) expre_uv
from ads_gsn_reduce_valid_goods t
left join dim.dim_vova_goods g on g.goods_id = t.goods_id
left join dwd.dwd_vova_log_goods_impression gi on gi.virtual_goods_id = g.virtual_goods_id
where gi.pt>=t.start_time and gi.pt>='$pre_2w' and gi.pt<=t.end_time and gi.pt<='$pt' and gi.dp='vova' and gi.platform='mob'
group by t.goods_id,t.add_cycle
) t1 on g.goods_id = t1.goods_id and g.add_cycle = t1.add_cycle
left join
(
select
t.goods_id,
t.add_cycle,
sum(p.goods_number) sales_order,
count(distinct device_id) payed_uv
from ads_gsn_reduce_valid_goods t
left join dwd.dwd_vova_fact_pay p on t.goods_id = p.goods_id
where to_date(p.pay_time)>=t.start_time and to_date(p.pay_time)<=t.end_time
and to_date(p.pay_time)<='$pt' and to_date(p.pay_time)>='$pre_2w'
and p.datasource = 'vova' and p.platform in ('ios','android')
group by t.goods_id,t.add_cycle
) t2 on g.goods_id = t2.goods_id and g.add_cycle =t2.add_cycle;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql  --conf "spark.app.name=ads_vova_gsn_reduce_valid_goods_zhangyin"  --conf "spark.dynamicAllocation.maxExecutors=100" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi