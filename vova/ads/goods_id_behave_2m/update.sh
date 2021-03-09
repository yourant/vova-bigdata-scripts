#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pt=$(date -d "-1 day" +%Y-%m-%d)
fi
pre_2m=`date -d "59 day ago ${pt}" +%Y-%m-%d`

sql="
with ads_goods_id_behave_2m_record as
(
select
goods_id,
virtual_goods_id,
goods_sn,
create_time
from
(
select
g.goods_id,
g.virtual_goods_id,
g.goods_sn,
gs.create_time,
row_number() OVER (PARTITION BY g.goods_id ORDER BY gs.create_time DESC) AS rank
from dim.dim_vova_goods g
left join ods_vova_vts.ods_vova_goods_on_sale_record gs on g.goods_id = gs.goods_id
where g.is_on_sale =1 and (gs.action ='on' or gs.action is null)
) t1 where rank =1
),
ads_goods_id_behave_2m_expre as
(
select
goods_id,
virtual_goods_id,
goods_sn,
date_diff,
count(distinct device_id) expre_uv,
sum(expre) expre
from
(
select
gi.virtual_goods_id,
g.goods_id,
g.goods_sn,
g.create_time,
gi.device_id,
1 expre,
case when g.create_time is null or datediff('$pt',g.create_time)>=60 then 60
     else datediff('$pt',g.create_time)+1 end date_diff
from dwd.dwd_vova_log_goods_impression gi
join ads_goods_id_behave_2m_record g on g.virtual_goods_id=gi.virtual_goods_id
where gi.pt>='$pre_2m' and gi.pt >=if(g.create_time is not null,to_date(g.create_time),'1970-01-01')
and dp ='vova' and platform='mob'
) t group by goods_id,virtual_goods_id,goods_sn,date_diff
),
ads_goods_id_behave_2m_clk as
(
select
goods_id,
virtual_goods_id,
goods_sn,
date_diff,
count(distinct device_id) clk_uv,
sum(clk) clk
from
(
select
gi.virtual_goods_id,
g.goods_id,
g.goods_sn,
g.create_time,
gi.device_id,
1 clk,
case when g.create_time is null or datediff('$pt',g.create_time)>=60 then 60
     else datediff('$pt',g.create_time)+1 end date_diff
from dwd.dwd_vova_log_goods_click gi
join ads_goods_id_behave_2m_record g on g.virtual_goods_id=gi.virtual_goods_id
where gi.pt>='$pre_2m' and gi.pt >=if(g.create_time is not null,to_date(g.create_time),'1970-01-01')
and dp ='vova' and platform='mob'
) t group by goods_id,virtual_goods_id,goods_sn,date_diff
),
ads_goods_id_behave_2m_cart as
(
select
goods_id,
virtual_goods_id,
goods_sn,
date_diff,
count(device_id) cart_pv
from
(
select
g.virtual_goods_id,
g.goods_id,
g.goods_sn,
g.create_time,
gi.device_id,
case when g.create_time is null or datediff('$pt',g.create_time)>=60 then 60
     else datediff('$pt',g.create_time)+1 end date_diff
from dwd.dwd_vova_log_common_click gi
join ads_goods_id_behave_2m_record g on g.virtual_goods_id=gi.element_id
where gi.pt>='$pre_2m' and gi.pt >=if(g.create_time is not null,to_date(g.create_time),'1970-01-01')
and dp ='vova' and platform='mob' and gi.element_name='pdAddToCartSuccess'
) t group by goods_id,virtual_goods_id,goods_sn,date_diff
),
ads_goods_id_behave_2m_gmv as
(
select
goods_id,
virtual_goods_id,
goods_sn,
date_diff,
sum(goods_number) sales_order,
sum(goods_number*shop_price+shipping_fee) gmv,
count(distinct buyer_id) payed_uv
from
(
select
g.virtual_goods_id,
g.goods_id,
g.goods_sn,
g.create_time,
gi.goods_number,
gi.shop_price,
gi.shipping_fee,
gi.buyer_id,
case when g.create_time is null or datediff('$pt',g.create_time)>=60 then 60
     else datediff('$pt',g.create_time)+1 end date_diff
from dwd.dwd_vova_fact_pay gi
join ads_goods_id_behave_2m_record g on g.goods_id=gi.goods_id
where to_date(pay_time)>='$pre_2m' and to_date(pay_time) >=if(g.create_time is not null,to_date(g.create_time),'1970-01-01')
and datasource ='vova' and platform in ('ios','android')
) t group by goods_id,virtual_goods_id,goods_sn,date_diff
)

insert overwrite table ads.ads_vova_goods_id_behave_2m PARTITION (pt = '$pt')
select
/*+ REPARTITION(5) */
t1.goods_id,
t1.virtual_goods_id,
t1.goods_sn,
t1.date_diff,
t1.expre_uv,
t1.expre,
t1.expre/t1.date_diff avg_expre,
nvl(t2.clk_uv,0) clk_uv,
nvl(t2.clk,0) clk,
nvl(t2.clk/t1.date_diff,0) avg_clk,
nvl(t3.cart_pv,0) cart_pv,
nvl(t3.cart_pv/t1.date_diff,0) avg_cart_pv,
nvl(t4.sales_order,0) sales_order,
nvl(t4.sales_order/t1.date_diff,0) avg_sales_order,
nvl(t4.gmv,0) gmv,
nvl(t4.gmv/t1.date_diff,0) avg_gmv,
nvl(t4.sales_order/t2.clk,0) avg_sor_div_clk,
nvl(t2.clk/t1.expre,0) ctr,
nvl(t4.payed_uv/t1.expre_uv,0) cr,
nvl(t4.gmv/t2.clk_uv * t2.clk/t1.expre,0) gcr,
nvl(t3.cart_pv/t2.clk,0) cart_pv_div_clk
from ads_goods_id_behave_2m_expre t1
left join ads_goods_id_behave_2m_clk t2 on t1.goods_id = t2.goods_id
left join ads_goods_id_behave_2m_cart t3 on t1.goods_id = t3.goods_id
left join ads_goods_id_behave_2m_gmv t4 on t1.goods_id = t4.goods_id;
"

spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=ads_vova_goods_id_behave_2m_zhangyin" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

