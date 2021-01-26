#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

sql="
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
from dwd.fact_log_goods_click goods_click 
join dwd.dim_goods dim_goods
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
from dwd.fact_log_goods_impression goods_impression 
join dwd.dim_goods dim_goods
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
from dwd.fact_pay
join dwd.dim_goods dim_goods on fact_pay.goods_id = dim_goods.goods_id
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
from dwd.fact_log_common_click c
left join dwd.dim_goods g on cast(c.element_id as bigint) = g.virtual_goods_id
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
from dwd.dim_order_goods order_goods
join dwd.dim_goods dim_goods on order_goods.goods_id = dim_goods.goods_id
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
from dwd.fact_log_goods_click goods_click 
join dwd.dim_goods dim_goods
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
from dwd.fact_log_goods_click goods_click 
join dwd.dim_goods dim_goods
on goods_click.virtual_goods_id = dim_goods.virtual_goods_id 
where goods_click.pt='${cur_date}' 
and dim_goods.mct_id = 26414
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
insert overwrite table rpt.rpt_shop_compare PARTITION (pt = '${cur_date}')
select
self_shop.cur_date,
self_shop.datasource,
self_shop.region_code,
self_shop.platform,
self_shop.first_cat_name,
self_shop.brand,
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
from rpt.rpt_selfshop_added_analysis self_shop
join all_shop_data all_shop
on self_shop.cur_date = all_shop.cur_date
and self_shop.datasource = all_shop.datasource
and self_shop.region_code = all_shop.region_code
and self_shop.platform = all_shop.platform
and self_shop.first_cat_name = all_shop.first_cat_name 
and self_shop.brand = all_shop.brand
join tmp8
on self_shop.datasource = tmp8.datasource
and self_shop.region_code = tmp8.region_code
and self_shop.platform = tmp8.platform
and self_shop.first_cat_name = tmp8.first_cat_name
and self_shop.brand = tmp8.brand
"

#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=30" --conf "spark.dynamicAllocation.initialExecutors=60" --conf "spark.app.name=rpt_shop_compare" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

