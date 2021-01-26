#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

#rpt_checkout
sql="
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=1000;
insert overwrite table dwb.dwb_vova_selfshop_added_ana PARTITION (pt)
select
o.action_date,
o.datasource,
o.region_code,
o.platform,
o.first_cat_name,
o.is_brand,
o.impression,
o.click,
cast(o.click as BIGINT) / cast(o.impression as BIGINT) ctr,    --ctr
impression_data.expouse_uv,  --曝光uv
click_data.click_uv,--点击uv
o.gmv,
concat(round(o.gmv * 100 / impression_data.expouse_uv,2),'%') expouse_income,    --曝光收益
cart.cart, --加购数
cart.cart_uv, --加购UV
concat(round(nvl(cart.cart_uv * 100 / click_data.click_uv,0),2),'%') cart_rate, --加购率
orde.order_uv, --下单uv
orde.order_id_count, --下单数
orde.order_goods_id_count,
concat(round(orde.order_uv * 100 / cart.cart_uv,2),'%') order_rate,    --下单率
o.paid_order_cnt,
o.paid_buyer_cnt,
concat(round(o.paid_buyer_cnt * 100 / orde.order_uv,2),'%') pay_rate,    --支付率
concat(round(o.paid_buyer_cnt * 100 / impression_data.expouse_uv,2),'%') total_change_rate,    --总转化率
nvl(rebuy.rebuy_uv,0) rebuy_uv, --复购UV
concat(round(nvl(rebuy.rebuy_uv,0) * 100 / o.paid_buyer_cnt,2),'%') rebay_rate,    --复购率
o.action_date pt
from (select * ,case
       when self_mct_name = 'Airyclub' THEN 26414
       when self_mct_name = 'SuperAC' THEN 11630
       when self_mct_name = 'VogueFD' THEN 36655
       when self_mct_name = 'dearbuys' THEN 61017
       when self_mct_name = 'shejoys' THEN 61028
       when self_mct_name = 'vvshein' THEN 61235
       when self_mct_name = 'SuperEC' THEN 61310
       else self_mct_name
       end as mct_id from dwb.dwb_vova_self_operated_merchant where goods_id = 'all' and action_date <= '${cur_date}' and action_date >= date_sub('${cur_date}', 7)
and self_mct_name = 'all') o
left join (
select
       nvl(nvl(datasource, 'NA'), 'all')     AS datasource,
       nvl(nvl(geo_country, 'NA'), 'all')  AS region_code,
       nvl(nvl(platform, 'NA'), 'all')  AS platform,
       nvl(nvl(first_cat_name, 'NA'), 'all') AS first_cat_name,
       nvl(if(brand_id >0 ,'Y', 'N'), 'all')        AS is_brand,
       nvl(mct_id, 'all')  AS mct_id,
       nvl(date(pt), 'all')         AS pay_date,
       nvl(goods_id, 'all')                  AS goods_id,
        count(distinct device_id) as expouse_uv
       from

(
SELECT dg.goods_id,
       dg.first_cat_name,
       dg.mct_id,
       dg.brand_id,
       log.datasource,
       log.geo_country,
       log.pt,
        log.device_id,
        case
           when log.platform = 'pc' then 'pc'
           when log.platform = 'web' then 'mob'
           when log.platform = 'mob' and log.os_type = 'android' then 'android'
           when log.platform = 'mob' and log.os_type = 'ios' then 'ios'
           else ''
           end                                            as platform
FROM dwd.dwd_vova_log_goods_impression log
inner join dim.dim_vova_goods dg on dg.virtual_goods_id = log.virtual_goods_id
WHERE log.pt <= '${cur_date}' and log.pt >= date_sub('${cur_date}', 7)
  AND dg.mct_id in (26414, 11630, 36655,61017,61028,61235,61310) ) temp
GROUP BY CUBE (nvl(datasource, 'NA'), nvl(geo_country, 'NA'), nvl(platform, 'NA'),goods_id,nvl(first_cat_name, 'NA'),date(pt),mct_id, if(brand_id >0 ,'Y', 'N'))
) impression_data ON o.datasource = impression_data.datasource
    AND o.region_code = impression_data.region_code
    AND o.platform = impression_data.platform
    AND o.first_cat_name = impression_data.first_cat_name
    AND o.action_date = impression_data.pay_date
    AND o.goods_id = impression_data.goods_id
    AND o.mct_id = impression_data.mct_id
    AND o.is_brand = impression_data.is_brand
left join
(
select
       nvl(nvl(datasource, 'NA'), 'all')     AS datasource,
       nvl(nvl(geo_country, 'NA'), 'all')  AS region_code,
       nvl(nvl(platform, 'NA'), 'all')  AS platform,
       nvl(mct_id, 'all')  AS mct_id,
       nvl(if(brand_id >0 ,'Y', 'N'), 'all')        AS is_brand,
       nvl(nvl(first_cat_name, 'NA'), 'all') AS first_cat_name,
       nvl(date(pt), 'all')         AS pay_date,
       nvl(goods_id, 'all')                  AS goods_id,
        count(distinct device_id) as click_uv
       from

(
SELECT dg.goods_id,
       dg.first_cat_name,
       dg.mct_id,
       dg.brand_id,
       log.datasource,
       log.geo_country,
       log.pt,
log.device_id,
        case
           when log.platform = 'pc' then 'pc'
           when log.platform = 'web' then 'mob'
           when log.platform = 'mob' and log.os_type = 'android' then 'android'
           when log.platform = 'mob' and log.os_type = 'ios' then 'ios'
           else ''
           end                                            as platform
FROM dwd.dwd_vova_log_goods_click log
inner join dim.dim_vova_goods dg on dg.virtual_goods_id = log.virtual_goods_id
WHERE log.pt <= '${cur_date}' and log.pt >= date_sub('${cur_date}', 7)
  AND dg.mct_id in (26414, 11630, 36655,61017,61028,61235,61310) ) temp
GROUP BY CUBE (nvl(datasource, 'NA'), nvl(geo_country, 'NA'), nvl(platform, 'NA'),goods_id,nvl(first_cat_name, 'NA'),date(pt),mct_id, if(brand_id >0 ,'Y', 'N'))
) click_data ON o.datasource = click_data.datasource
    AND o.region_code = click_data.region_code
    AND o.platform = click_data.platform
    AND o.first_cat_name = click_data.first_cat_name
    AND o.action_date = click_data.pay_date
    AND o.goods_id = click_data.goods_id
    AND o.mct_id = click_data.mct_id
    AND o.is_brand = click_data.is_brand
left join (
select
nvl(datasource,'all') datasource,
nvl(region_code,'all')  region_code,
nvl(platform,'all') platform,
nvl(first_cat_name,'all') first_cat_name,
nvl(brand,'all') is_brand,
nvl(mct_id,'all') mct_id,
nvl(goods_id,'all') goods_id,
nvl(pt,'all') pt,
count(*) / count(distinct device_id) cart,
count(distinct device_id) cart_uv
from (
select
nvl(c.datasource,'NA') datasource,
nvl(c.country,'NA')  region_code,
nvl(c.os_type,'NA') platform,
nvl(g.mct_id, 'NA')  AS mct_id,
nvl(g.first_cat_name,'NA') first_cat_name,
if(g.brand_id > 0,'Y','N') brand,
nvl(g.goods_id,'NA') goods_id,
nvl(c.pt,'NA') pt,
device_id
from dwd.dwd_vova_log_common_click c
left join dim.dim_vova_goods g on cast(c.element_id as bigint) = g.virtual_goods_id
where pt <= '${cur_date}' and pt >= date_sub('${cur_date}', 7) and element_name = 'pdAddToCartSuccess' and g.mct_id in (26414, 11630, 36655,61017,61028,61235,61310)
) tmp5_1
group by cube (tmp5_1.datasource,tmp5_1.region_code,tmp5_1.platform,tmp5_1.first_cat_name,tmp5_1.brand,tmp5_1.pt,tmp5_1.mct_id,tmp5_1.goods_id)
) cart ON o.datasource = cart.datasource
    AND o.region_code = cart.region_code
    AND o.platform = cart.platform
    AND o.first_cat_name = cart.first_cat_name
    and o.action_date = cart.pt
    AND o.goods_id = cart.goods_id
    AND o.mct_id = cart.mct_id
    AND o.is_brand = cart.is_brand
left join (
select
nvl(datasource,'all') datasource,
nvl(region_code,'all') region_code,
nvl(platform,'all') platform,
nvl(first_cat_name,'all') first_cat_name,
nvl(brand,'all') brand,
nvl(mct_id,'all') mct_id,
nvl(goods_id,'all') goods_id,
nvl(order_time,'all') order_time,
count(distinct buyer_id) order_uv,
count(distinct order_id) order_id_count,
count(distinct order_goods_id) order_goods_id_count
from (
select
nvl(order_goods.datasource,'NA') datasource,
nvl(order_goods.region_code,'NA') region_code,
nvl(order_goods.platform,'NA') platform,
nvl(order_goods.first_cat_name,'NA') first_cat_name,
nvl(order_goods.mct_id,'NA') mct_id,
nvl(order_goods.goods_id,'NA') goods_id,
nvl(to_date(order_time),'NA') order_time,
if(dim_goods.brand_id > 0,'Y','N') brand,
buyer_id,
order_id,
order_goods_id
from dim.dim_vova_order_goods order_goods
join dim.dim_vova_goods dim_goods on order_goods.goods_id = dim_goods.goods_id
where to_date(order_time) <= '${cur_date}' and to_date(order_time) >= date_sub('${cur_date}', 7)
and order_goods.mct_id in (26414, 11630, 36655,61017,61028,61235,61310)
) tmp6_1
group by cube (tmp6_1.datasource,tmp6_1.region_code,tmp6_1.platform,tmp6_1.first_cat_name,tmp6_1.order_time,tmp6_1.brand,tmp6_1.mct_id,tmp6_1.goods_id)
) orde ON o.datasource = orde.datasource
    AND o.region_code = orde.region_code
    AND o.platform = orde.platform
    AND o.first_cat_name = orde.first_cat_name
    AND o.goods_id = orde.goods_id
    and o.action_date = orde.order_time
    AND o.mct_id = orde.mct_id
    AND o.is_brand = orde.brand
left join (
select
nvl(datasource,'all') datasource,
nvl(region_code,'all')  region_code,
nvl(platform,'all') platform,
nvl(first_cat_name,'all') first_cat_name,
nvl(mct_id,'all') mct_id,
nvl(goods_id,'all') goods_id,
nvl(pay_time,'all') pay_time,
nvl(brand,'all') brand,
count(distinct buyer_id) rebuy_uv
from (
select
nvl(pay.datasource,'NA') datasource,
nvl(pay.region_code,'NA')  region_code,
nvl(pay.platform,'NA') platform,
nvl(pay.first_cat_name,'NA') first_cat_name,
nvl(pay.mct_id,'NA') mct_id,
nvl(pay.goods_id,'NA') goods_id,
nvl(to_date(pay.pay_time),'NA') pay_time,
if(dim_goods.brand_id > 0,'Y','N') brand,
pay.buyer_id
from dwd.dwd_vova_fact_pay  pay
join dwd.dwd_vova_fact_pay pay_history
--on pay.goods_id = pay_history.goods_id
on pay.buyer_id = pay_history.buyer_id
and  pay.mct_id = pay_history.mct_id
join dim.dim_vova_goods dim_goods on pay.goods_id = dim_goods.goods_id
where to_date(pay.pay_time) <= '${cur_date}' and to_date(pay.pay_time) >= date_sub('${cur_date}', 7)
and '${cur_date}' > pay_history.pay_time
and to_date(pay.pay_time) > to_date(pay_history.pay_time)
--and pay_history.pay_time > date_sub('${cur_date}',30)
and pay.mct_id in (26414, 11630, 36655,61017,61028,61235,61310)
and pay_history.mct_id in (26414, 11630, 36655,61017,61028,61235,61310)
) tmp7_1
group by cube (tmp7_1.datasource,tmp7_1.region_code,tmp7_1.platform,tmp7_1.first_cat_name,tmp7_1.pay_time,tmp7_1.brand,tmp7_1.mct_id,tmp7_1.goods_id)
) rebuy ON o.datasource = rebuy.datasource
    AND o.region_code = rebuy.region_code
    AND o.platform = rebuy.platform
    AND o.first_cat_name = rebuy.first_cat_name
    and o.action_date = rebuy.pay_time
    AND o.goods_id = rebuy.goods_id
    AND o.mct_id = rebuy.mct_id
    AND o.is_brand = rebuy.brand
"


spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=30" \
--conf "spark.dynamicAllocation.initialExecutors=30" \
--conf "spark.app.name=dwb_vova_selfshop_added_ana" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=300000" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

sql2="
--店铺大盘对比数据
with tmp1 as (
select
nvl(goods_click.datasource,'NA') datasource,
nvl(goods_click.geo_country,'NA') region_code,
nvl(goods_click.os_type,'NA') platform,
nvl(dim_goods.first_cat_name,'NA') first_cat_name,
if(dim_goods.brand_id > 0,'yes','no') brand,
0 impression,
1 click
from dwd.dwd_vova_log_goods_click goods_click
join dim.dim_vova_goods dim_goods
on goods_click.virtual_goods_id = dim_goods.virtual_goods_id
where goods_click.pt='${cur_date}'
),
tmp2 as (
select
nvl(goods_impression.datasource,'NA') datasource,
nvl(goods_impression.geo_country,'NA') region_code,
nvl(goods_impression.os_type,'NA') platform,
nvl(dim_goods.first_cat_name,'NA') first_cat_name,
if(dim_goods.brand_id > 0,'yes','no') brand,
1 impression,
0 click
from dwd.dwd_vova_log_goods_impression goods_impression
join dim.dim_vova_goods dim_goods
on goods_impression.virtual_goods_id = dim_goods.virtual_goods_id
where goods_impression.pt='${cur_date}'
),
tmp3 as (
select
nvl(datasource,'all') datasource,
nvl(region_code,'all') region_code,
nvl(platform,'all') platform,
nvl(first_cat_name,'all') first_cat_name,
nvl(brand,'all') brand,
sum(impression) impression,
sum(click) clicks,
nvl(sum(click) / sum(impression), 0) as ctr
from
(
select
datasource,
region_code,
platform,
first_cat_name,
brand,
impression,
click
from tmp1
union all
select
datasource,
region_code,
platform,
first_cat_name,
brand,
impression,
click
from tmp2
) tmp
group by cube (tmp.datasource,tmp.region_code,tmp.platform,tmp.first_cat_name,tmp.brand)
),
--GMV,支付数量
tmp4 as (
select
nvl(datasource,'all') datasource,
nvl(region_code,'all')  region_code,
nvl(platform,'all') platform,
nvl(first_cat_name,'all') first_cat_name,
nvl(brand,'all') brand,
sum(goods_number * shop_price + shipping_fee) gmv,
count(distinct order_goods_id) pay_nums
from (
select
nvl(fact_pay.datasource,'NA') datasource,
nvl(fact_pay.region_code,'NA')  region_code,
nvl(fact_pay.platform,'NA') platform,
nvl(fact_pay.first_cat_name,'NA') first_cat_name,
if(dim_goods.brand_id > 0,'yes','no') brand,
fact_pay.goods_number,
fact_pay.shop_price,
fact_pay.shipping_fee,
fact_pay.order_goods_id
from dwd.dwd_vova_fact_pay fact_pay
join dim.dim_vova_goods dim_goods on fact_pay.goods_id = dim_goods.goods_id
where to_date(order_time)='${cur_date}'
) tmp4_1
group by cube (datasource,region_code,platform,first_cat_name,brand)
),
--加购数
tmp5 as (
select
nvl(datasource,'all') datasource,
nvl(region_code,'all')  region_code,
nvl(platform,'all') platform,
nvl(first_cat_name,'all') first_cat_name,
nvl(brand,'all') brand,
count(*) / count(distinct device_id) cart
from (
select
nvl(c.datasource,'NA') datasource,
nvl(c.country,'NA')  region_code,
nvl(c.os_type,'NA') platform,
nvl(g.first_cat_name,'NA') first_cat_name,
if(g.brand_id > 0,'yes','no') brand,
c.device_id
from dwd.dwd_vova_log_common_click c
left join dim.dim_vova_goods g on cast(c.element_id as bigint) = g.virtual_goods_id
where pt='${cur_date}' and element_name = 'pdAddToCartSuccess'
) tmp5_1
group by cube (datasource,region_code,platform,first_cat_name,brand)
),
--订单量
tmp6 as (
select
nvl(datasource,'all') datasource,
nvl(region_code,'all') region_code,
nvl(platform,'all') platform,
nvl(first_cat_name,'all') first_cat_name,
nvl(brand,'all') brand,
count(distinct order_goods_id) order_count
from (
select
nvl(order_goods.datasource,'NA') datasource,
nvl(order_goods.region_code,'NA') region_code,
nvl(order_goods.platform,'NA') platform,
nvl(order_goods.first_cat_name,'NA') first_cat_name,
if(dim_goods.brand_id > 0,'yes','no') brand,
order_goods.order_goods_id
from dim.dim_vova_order_goods order_goods
join dim.dim_vova_goods dim_goods on order_goods.goods_id = dim_goods.goods_id
where to_date(order_time) = '${cur_date}'
)
group by cube (datasource,region_code,platform,first_cat_name,brand)
),
--大盘bestselling,detail_also_like,search_result
tmp7 as (
select
nvl(datasource,'all') datasource,
nvl(region_code,'all') region_code,
nvl(platform,'all') platform,
nvl(first_cat_name,'all') first_cat_name,
nvl(brand,'all') brand,
count(distinct bestselling) - 1 bestselling,
count(distinct detail_also_like) - 1 detail_also_like,
count(distinct search_result) - 1 search_result
from (
select
nvl(goods_click.datasource,'NA') datasource,
nvl(goods_click.geo_country,'NA') region_code,
nvl(goods_click.os_type,'NA') platform,
nvl(dim_goods.first_cat_name,'NA') first_cat_name,
if(dim_goods.brand_id > 0,'yes','no') brand,
case when goods_click.list_type = '/popular' and goods_click.page_code = 'homepage' then goods_click.device_id
else 0
end bestselling,
case when goods_click.list_type = '/detail_also_like' and goods_click.page_code = 'product_detail' then goods_click.device_id
else 0
end detail_also_like,
case when goods_click.list_type = '/search_result' and goods_click.page_code = 'search_result' then goods_click.device_id
else 0
end search_result
from dwd.dwd_vova_log_goods_click goods_click
join dim.dim_vova_goods dim_goods
on goods_click.virtual_goods_id = dim_goods.virtual_goods_id
where goods_click.pt='${cur_date}'
)
group by cube(datasource,region_code,platform,first_cat_name,brand)
),
--自营bestselling,detail_also_like,search_result
tmp8 as (
select
nvl(datasource,'all') datasource,
nvl(region_code,'all') region_code,
nvl(platform,'all') platform,
nvl(first_cat_name,'all') first_cat_name,
nvl(brand,'all') brand,
count(distinct bestselling) - 1 bestselling,
count(distinct detail_also_like) - 1 detail_also_like,
count(distinct search_result) - 1 search_result
from (
select
nvl(goods_click.datasource,'NA') datasource,
nvl(goods_click.geo_country,'NA') region_code,
nvl(goods_click.os_type,'NA') platform,
nvl(dim_goods.first_cat_name,'NA') first_cat_name,
if(dim_goods.brand_id > 0,'yes','no') brand,
case when goods_click.list_type = '/popular' and goods_click.page_code = 'homepage' then goods_click.device_id
else 0
end bestselling,
case when goods_click.list_type = '/detail_also_like' and goods_click.page_code = 'product_detail' then goods_click.device_id
else 0
end detail_also_like,
case when goods_click.list_type = '/search_result' and goods_click.page_code = 'search_result' then goods_click.device_id
else 0
end search_result
from dwd.dwd_vova_log_goods_click goods_click
join dim.dim_vova_goods dim_goods
on goods_click.virtual_goods_id = dim_goods.virtual_goods_id
where goods_click.pt='${cur_date}'
and dim_goods.mct_id in (26414, 11630, 36655,61017,61028,61235,61310)
)
group by cube(datasource,region_code,platform,first_cat_name,brand)
),
--汇总大盘数据
all_shop_data as (
select
'${cur_date}' cur_date,
tmp3.datasource,
tmp3.region_code,
tmp3.platform,
tmp3.first_cat_name,
nvl(tmp3.brand,'all') brand,
tmp3.impression,
tmp3.clicks,	--点击数
tmp3.ctr,
tmp4.gmv,
tmp5.cart, --加购数
tmp6.order_count, --订单量
tmp4.pay_nums,	--支付数
tmp7.bestselling,	--bestselling
tmp7.detail_also_like,	--detail_also_like
tmp7.search_result	--search_result
from tmp3
join tmp4
on tmp3.datasource = tmp4.datasource
and tmp3.region_code = tmp4.region_code
and tmp3.platform = tmp4.platform
and tmp3.first_cat_name = tmp4.first_cat_name
and tmp3.brand = tmp4.brand
join tmp5
on tmp3.datasource = tmp5.datasource
and tmp3.region_code = tmp5.region_code
and tmp3.platform = tmp5.platform
and tmp3.first_cat_name = tmp5.first_cat_name
and tmp3.brand = tmp5.brand
join tmp6
on tmp3.datasource = tmp6.datasource
and tmp3.region_code = tmp6.region_code
and tmp3.platform = tmp6.platform
and tmp3.first_cat_name = tmp6.first_cat_name
and tmp3.brand = tmp6.brand
join tmp7
on tmp3.datasource = tmp7.datasource
and tmp3.region_code = tmp7.region_code
and tmp3.platform = tmp7.platform
and tmp3.first_cat_name = tmp7.first_cat_name
and tmp3.brand = tmp7.brand
)
insert overwrite table dwb.dwb_vova_shop_com PARTITION (pt = '${cur_date}')
select
self_shop.cur_date,
self_shop.datasource,
self_shop.region_code,
self_shop.platform,
self_shop.first_cat_name,
self_shop.is_brand,
concat(round(self_shop.impression * 100 / all_shop.impression,2),'%') impression_persent,	--曝光占比
concat(round(self_shop.clks * 100 / all_shop.clicks,2),'%') clks_persent,	--点击占比
concat(round(self_shop.gmv * 100 / all_shop.gmv,2),'%') gmv_persent,	--gmv占比
concat(round((all_shop.ctr - self_shop.ctr) * 100,2),'%') ctr_diff,	--ctr占比
all_shop.impression - self_shop.impression impression_diff,	--曝光差异
concat(round(tmp8.bestselling * 100 / all_shop.bestselling,2),'%')  bestselling_persent,	--bestselling占比
concat(round(tmp8.detail_also_like * 100 / all_shop.detail_also_like,2),'%')  yourlike_persent,	--yourlike占比
concat(round(tmp8.search_result * 100 / all_shop.search_result,2),'%')  result_persent,	--search_result占比
concat(round(self_shop.cart * 100 / all_shop.cart,2),'%') cart_persent,	--加购占比
concat(round(self_shop.order_count * 100 / all_shop.order_count,2),'%') order_persent,	--订单占比
concat(round(self_shop.pay_nums * 100 / all_shop.pay_nums,2),'%') pay_persent	--支付占比
from dwb.dwb_vova_selfshop_added_ana self_shop
join all_shop_data all_shop
on self_shop.cur_date = all_shop.cur_date
and self_shop.datasource = all_shop.datasource
and self_shop.region_code = all_shop.region_code
and self_shop.platform = all_shop.platform
and self_shop.first_cat_name = all_shop.first_cat_name
and self_shop.is_brand = all_shop.brand
join tmp8
on self_shop.datasource = tmp8.datasource
and self_shop.region_code = tmp8.region_code
and self_shop.platform = tmp8.platform
and self_shop.first_cat_name = tmp8.first_cat_name
and self_shop.is_brand = tmp8.brand
"


spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=30" \
--conf "spark.dynamicAllocation.initialExecutors=30" \
--conf "spark.app.name=dwb_vova_shop_com" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=300000" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql2"


#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

