#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
sql="

insert overwrite table dwb.dwb_zq_goods_cat_behave  PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(5) */
'${cur_date}' AS event_date,
'TRIGRAM' AS domain_group,
datasource,
original_source,
first_cat_name,
second_cat_name,
sum(expres) as expres,
sum(clks) as clks,
sum(cart_pv) as cart_pv,
sum(sale_cnt) as sale_cnt,
sum(gmv) as gmv
from
(
select
nvl(nvl(log.datasource,'NA'), 'all') datasource,
nvl(nvl(original_source,'NA'), 'all') original_source,
nvl(nvl(first_cat_name,'NA'), 'all') first_cat_name,
nvl(nvl(second_cat_name,'NA'), 'all') second_cat_name,
count(*) as expres,
0 as clks,
0 as cart_pv,
0 as sale_cnt,
0 as gmv
from dwd.dwd_vova_log_goods_impression log
INNER JOIN dim.dim_zq_site zs on zs.datasource = log.datasource AND zs.domain_group IN ('FD', 'TRIGRAM')
inner join dim.dim_zq_goods fdg on fdg.virtual_goods_id = log.virtual_goods_id
WHERE log.pt='${cur_date}'
and log.dp = 'others'
and log.platform in('pc','web')
group by cube (nvl(log.datasource,'NA'), nvl(original_source,'NA'), nvl(first_cat_name,'NA'), nvl(second_cat_name,'NA'))

UNION ALL

select
nvl(nvl(log.datasource,'NA'), 'all') datasource,
nvl(nvl(original_source,'NA'), 'all') original_source,
nvl(nvl(first_cat_name,'NA'), 'all') first_cat_name,
nvl(nvl(second_cat_name,'NA'), 'all') second_cat_name,
0 as expres,
count(*) as clks,
0 as cart_pv,
0 as sale_cnt,
0 as gmv
from dwd.dwd_vova_log_goods_click log
INNER JOIN dim.dim_zq_site zs on zs.datasource = log.datasource AND zs.domain_group IN ('FD', 'TRIGRAM')
inner join dim.dim_zq_goods fdg on fdg.virtual_goods_id = log.virtual_goods_id
WHERE log.pt='${cur_date}'
and log.dp = 'others'
and log.platform in('pc','web')
group by cube (nvl(log.datasource,'NA'), nvl(original_source,'NA'), nvl(first_cat_name,'NA'), nvl(second_cat_name,'NA'))

UNION ALL

select
nvl(nvl(log.datasource,'NA'), 'all') datasource,
nvl(nvl(original_source,'NA'), 'all') original_source,
nvl(nvl(first_cat_name,'NA'), 'all') first_cat_name,
nvl(nvl(second_cat_name,'NA'), 'all') second_cat_name,
0 as expres,
0 as clks,
count(*) as cart_pv,
0 as sale_cnt,
0 as gmv
from dwd.dwd_vova_log_data log
INNER JOIN dim.dim_zq_site zs on zs.datasource = log.datasource AND zs.domain_group IN ('FD', 'TRIGRAM')
inner join dim.dim_zq_goods fdg on fdg.virtual_goods_id = log.element_id
WHERE log.pt='${cur_date}'
and log.element_name in ('AddToCartSuccess')
and log.page_code in ('product_detail')
and log.platform in('pc','web')
and log.dp = 'others'
group by cube (nvl(log.datasource,'NA'), nvl(original_source,'NA'), nvl(first_cat_name,'NA'), nvl(second_cat_name,'NA'))

UNION ALL
select
nvl(nvl(zog.datasource,'NA'), 'all') datasource,
nvl(nvl(fdg.original_source,'NA'), 'all') original_source,
nvl(nvl(fdg.first_cat_name,'NA'), 'all') first_cat_name,
nvl(nvl(fdg.second_cat_name,'NA'), 'all') second_cat_name,
0 as expres,
0 as clks,
0 as cart_pv,
sum(zog.goods_number) as sale_cnt,
sum(zog.goods_number * zog.shop_price) as gmv
from dim.dim_zq_order_goods zog
INNER JOIN dim.dim_zq_site zs on zs.datasource = zog.datasource AND zs.domain_group IN ('FD', 'TRIGRAM')
inner join dim.dim_zq_goods fdg on fdg.goods_id = zog.goods_id
WHERE zog.pay_status >= 1
AND date(zog.pay_time) = '${cur_date}'
group by cube (nvl(zog.datasource,'NA'), nvl(original_source,'NA'), nvl(first_cat_name,'NA'), nvl(second_cat_name,'NA'))
) fin
group by datasource, original_source, first_cat_name, second_cat_name

UNION ALL

select
/*+ REPARTITION(5) */
'${cur_date}' AS event_date,
'FN' AS domain_group,
datasource,
original_source,
first_cat_name,
second_cat_name,
sum(expres) as expres,
sum(clks) as clks,
sum(cart_pv) as cart_pv,
sum(sale_cnt) as sale_cnt,
sum(gmv) as gmv
from
(
select
nvl(nvl(log.datasource,'NA'), 'all') datasource,
nvl(nvl(original_source,'NA'), 'all') original_source,
nvl(nvl(first_cat_name,'NA'), 'all') first_cat_name,
nvl(nvl(second_cat_name,'NA'), 'all') second_cat_name,
count(*) as expres,
0 as clks,
0 as cart_pv,
0 as sale_cnt,
0 as gmv
from dwd.dwd_vova_log_goods_impression log
INNER JOIN dim.dim_zq_site zs on zs.datasource = log.datasource AND zs.domain_group IN ('FN')
inner join dim.dim_zq_goods fdg on fdg.virtual_goods_id = log.virtual_goods_id
WHERE log.pt='${cur_date}'
and log.dp = 'others'
and log.platform in('pc','web')
group by cube (nvl(log.datasource,'NA'), nvl(original_source,'NA'), nvl(first_cat_name,'NA'), nvl(second_cat_name,'NA'))

UNION ALL

select
nvl(nvl(log.datasource,'NA'), 'all') datasource,
nvl(nvl(original_source,'NA'), 'all') original_source,
nvl(nvl(first_cat_name,'NA'), 'all') first_cat_name,
nvl(nvl(second_cat_name,'NA'), 'all') second_cat_name,
0 as expres,
count(*) as clks,
0 as cart_pv,
0 as sale_cnt,
0 as gmv
from dwd.dwd_vova_log_goods_click log
INNER JOIN dim.dim_zq_site zs on zs.datasource = log.datasource AND zs.domain_group IN ('FN')
inner join dim.dim_zq_goods fdg on fdg.virtual_goods_id = log.virtual_goods_id
WHERE log.pt='${cur_date}'
and log.dp = 'others'
and log.platform in('pc','web')
group by cube (nvl(log.datasource,'NA'), nvl(original_source,'NA'), nvl(first_cat_name,'NA'), nvl(second_cat_name,'NA'))

UNION ALL

select
nvl(nvl(log.datasource,'NA'), 'all') datasource,
nvl(nvl(original_source,'NA'), 'all') original_source,
nvl(nvl(first_cat_name,'NA'), 'all') first_cat_name,
nvl(nvl(second_cat_name,'NA'), 'all') second_cat_name,
0 as expres,
0 as clks,
count(*) as cart_pv,
0 as sale_cnt,
0 as gmv
from dwd.dwd_vova_log_data log
INNER JOIN dim.dim_zq_site zs on zs.datasource = log.datasource AND zs.domain_group IN ('FN')
inner join dim.dim_zq_goods fdg on fdg.virtual_goods_id = log.element_id
WHERE log.pt='${cur_date}'
and log.element_name in ('AddToCartSuccess')
and log.page_code in ('product_detail')
and log.platform in('pc','web')
and log.dp = 'others'
group by cube (nvl(log.datasource,'NA'), nvl(original_source,'NA'), nvl(first_cat_name,'NA'), nvl(second_cat_name,'NA'))

UNION ALL
select
nvl(nvl(zog.datasource,'NA'), 'all') datasource,
nvl(nvl(fdg.original_source,'NA'), 'all') original_source,
nvl(nvl(fdg.first_cat_name,'NA'), 'all') first_cat_name,
nvl(nvl(fdg.second_cat_name,'NA'), 'all') second_cat_name,
0 as expres,
0 as clks,
0 as cart_pv,
sum(zog.goods_number) as sale_cnt,
sum(zog.goods_number * zog.shop_price) as gmv
from dim.dim_zq_order_goods zog
INNER JOIN dim.dim_zq_site zs on zs.datasource = zog.datasource AND zs.domain_group IN ('FN')
inner join dim.dim_zq_goods fdg on fdg.goods_id = zog.goods_id
WHERE zog.pay_status >= 1
AND date(zog.pay_time) = '${cur_date}'
group by cube (nvl(zog.datasource,'NA'), nvl(original_source,'NA'), nvl(first_cat_name,'NA'), nvl(second_cat_name,'NA'))
) fin
group by datasource, original_source, first_cat_name, second_cat_name
;


insert overwrite table dwb.dwb_zq_goods_sale_data PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
'${cur_date}' AS event_date,
'TRIGRAM' AS domain_group,
fdg.goods_id,
data.datasource,
fdg.original_source,
fdg.first_cat_name,
fdg.second_cat_name,
expres,
clks,
cart_pv,
sale_cnt,
gmv
from
(
select
datasource,
virtual_goods_id,
sum(expres) as expres,
sum(clks) as clks,
sum(cart_pv) as cart_pv,
sum(sale_cnt) as sale_cnt,
sum(gmv) as gmv
from
(
select
log.datasource,
log.virtual_goods_id,
count(*) as expres,
0 as clks,
0 as cart_pv,
0 as sale_cnt,
0 as gmv
from dwd.dwd_vova_log_goods_impression log
INNER JOIN dim.dim_zq_site zs on zs.datasource = log.datasource AND zs.domain_group IN ('FD', 'TRIGRAM')
WHERE log.pt='${cur_date}'
and log.platform in('pc','web')
and log.dp = 'others'
group by log.virtual_goods_id, log.datasource

UNION ALL

select
log.datasource,
log.virtual_goods_id,
0 as expres,
count(*) as clks,
0 as cart_pv,
0 as sale_cnt,
0 as gmv
from dwd.dwd_vova_log_goods_click log
INNER JOIN dim.dim_zq_site zs on zs.datasource = log.datasource AND zs.domain_group IN ('FD', 'TRIGRAM')
WHERE log.pt='${cur_date}'
and log.platform in('pc','web')
and log.dp = 'others'
group by log.virtual_goods_id, log.datasource

UNION ALL

select
log.datasource,
cast(log.element_id as bigint) as virtual_goods_id,
0 as expres,
0 as clks,
count(*) as cart_pv,
0 as sale_cnt,
0 as gmv
from dwd.dwd_vova_log_data log
INNER JOIN dim.dim_zq_site zs on zs.datasource = log.datasource AND zs.domain_group IN ('FD', 'TRIGRAM')
WHERE log.pt='${cur_date}'
and log.element_name in ('AddToCartSuccess')
and log.page_code in ('product_detail')
and log.platform in('pc','web')
and log.dp = 'others'
group by log.element_id, log.datasource

UNION ALL
select
zog.datasource,
fdg.virtual_goods_id,
0 as expres,
0 as clks,
0 as cart_pv,
sum(zog.goods_number) as sale_cnt,
sum(zog.goods_number * zog.shop_price) as gmv
from dim.dim_zq_order_goods zog
INNER JOIN dim.dim_zq_site zs on zs.datasource = zog.datasource AND zs.domain_group IN ('FD', 'TRIGRAM')
inner join dim.dim_zq_goods fdg on fdg.goods_id = zog.goods_id
WHERE zog.pay_status >= 1
AND date(zog.pay_time) = '${cur_date}'
group by fdg.virtual_goods_id, zog.datasource
) fin
group by virtual_goods_id, datasource
) data
inner join dim.dim_zq_goods fdg on fdg.virtual_goods_id = data.virtual_goods_id

UNION ALL

select
/*+ REPARTITION(1) */
'${cur_date}' AS event_date,
'FN' AS domain_group,
fdg.goods_id,
data.datasource,
fdg.original_source,
fdg.first_cat_name,
fdg.second_cat_name,
expres,
clks,
cart_pv,
sale_cnt,
gmv
from
(
select
datasource,
virtual_goods_id,
sum(expres) as expres,
sum(clks) as clks,
sum(cart_pv) as cart_pv,
sum(sale_cnt) as sale_cnt,
sum(gmv) as gmv
from
(
select
log.datasource,
log.virtual_goods_id,
count(*) as expres,
0 as clks,
0 as cart_pv,
0 as sale_cnt,
0 as gmv
from dwd.dwd_vova_log_goods_impression log
INNER JOIN dim.dim_zq_site zs on zs.datasource = log.datasource AND zs.domain_group IN ('FN')
WHERE log.pt='${cur_date}'
and log.platform in('pc','web')
and log.dp = 'others'
group by log.virtual_goods_id, log.datasource

UNION ALL

select
log.datasource,
log.virtual_goods_id,
0 as expres,
count(*) as clks,
0 as cart_pv,
0 as sale_cnt,
0 as gmv
from dwd.dwd_vova_log_goods_click log
INNER JOIN dim.dim_zq_site zs on zs.datasource = log.datasource AND zs.domain_group IN ('FN')
WHERE log.pt='${cur_date}'
and log.platform in('pc','web')
and log.dp = 'others'
group by log.virtual_goods_id, log.datasource

UNION ALL

select
log.datasource,
cast(log.element_id as bigint) as virtual_goods_id,
0 as expres,
0 as clks,
count(*) as cart_pv,
0 as sale_cnt,
0 as gmv
from dwd.dwd_vova_log_data log
INNER JOIN dim.dim_zq_site zs on zs.datasource = log.datasource AND zs.domain_group IN ('FN')
WHERE log.pt='${cur_date}'
and log.element_name in ('AddToCartSuccess')
and log.page_code in ('product_detail')
and log.platform in('pc','web')
and log.dp = 'others'
group by log.element_id, log.datasource

UNION ALL
select
zog.datasource,
fdg.virtual_goods_id,
0 as expres,
0 as clks,
0 as cart_pv,
sum(zog.goods_number) as sale_cnt,
sum(zog.goods_number * zog.shop_price) as gmv
from dim.dim_zq_order_goods zog
INNER JOIN dim.dim_zq_site zs on zs.datasource = zog.datasource AND zs.domain_group IN ('FN')
inner join dim.dim_zq_goods fdg on fdg.goods_id = zog.goods_id
WHERE zog.pay_status >= 1
AND date(zog.pay_time) = '${cur_date}'
group by fdg.virtual_goods_id, zog.datasource
) fin
group by virtual_goods_id, datasource
) data
inner join dim.dim_zq_goods fdg on fdg.virtual_goods_id = data.virtual_goods_id
;

"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" --conf "spark.sql.adaptive.shuffle.targetPostShuffleInputSize=128000000" --conf "spark.sql.adaptive.enabled=true" --conf "spark.app.name=dwb_zq_cat_goods" -e "$sql"
#hive -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

