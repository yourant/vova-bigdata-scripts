
insert overwrite table dwb.dwb_fd_category_data_analyze_rpt partition (pt='${pt}')
select
concat('${pt}',' 00:00:00') as reporte_date,
t1.project,
t1.category_name,
t1.country ,
t1.impression_num,
nvl(t2.advs_product_pv,0),
nvl(t3.click_num,0),
nvl(t3.click_num/t1.impression_num ,0)as ctr_rate,
nvl(t4.add_click_num/t7.product_impression_num ,0) as add_car_rate,
nvl(t5.sales_volume,0) as sales_volume,
nvl(t5.sales,0) as sales,
nvl(t5.order_valume,0) as order_numbers,
nvl(t5.sales_volume/t5.order_valume ,0)as avg_order_fees,
nvl(t6.link_order_num/t5.order_valume,0) as link_order_rate,
nvl(t5.order_valume/t1.impression_num ,0)as all_ctr_rate
from
----根据国家，品类，项目计算指定页面下的曝光量
(select project,category_name,country ,count(*) as impression_num from dwd.dwd_fd_category_data_analyze_goods_event_detail
where project in ('floryday','airydress')
and event_name='goods_impression'
-- 指定的几个品类
and category_name in ('Dress','Blouses','Shoes','Coats','Swimwear','Sweaters','Pants&Leggings','Sweatshirts','T-shirts')
-- 指定页面的曝光量
and (
(source_type='PC' and page_code in ('list','sale','search','finalSaleV2List','promotion','activityList','homepage'))
or
(source_type='APP' and page_code in ('list','sale','topPicks','searchResult','buywithpoints','activityList','homepage'))
or
(source_type='H5' and page_code in ('list','sale','hotClearance','promotion','homepage','activityList'))
)
group by  project,category_name,country) t1

left join
--广告商品详情页pv
(select project,category_name,country ,count(*) as advs_product_pv from dwd.dwd_fd_category_data_analyze_goods_event_detail
where project in ('floryday','airydress') and mkt_source <> '' and page_code='product'
group by  project,category_name,country) t2
on t1.project=t2.project and t1.category_name=t2.category_name and t1.country=t2.country

left join
--商品的所有点击量
(select project,category_name,country ,count(*) as click_num from dwd.dwd_fd_category_data_analyze_goods_event_detail
where project in ('floryday','airydress')
-- 点击事件
and event_name='goods_click'
-- 指定的几个品类
and category_name in ('Dress','Blouses','Shoes','Coats','Swimwear','Sweaters','Pants&Leggings','Sweatshirts','T-shirts')
-- 指定页面的点击量
and (
(source_type='PC' and page_code in ('list','sale','search','finalSaleV2List','promotion','activityList','homepage'))
or
(source_type='APP' and page_code in ('list','sale','topPicks','searchResult','buywithpoints','activityList','homepage'))
or
(source_type='H5' and page_code in ('list','sale','hotClearance','promotion','homepage','activityList'))
)
group by  project,category_name,country) t3
on t1.project=t3.project and t1.category_name=t3.category_name and t1.country=t3.country

left join
--商品的加车点击量
(select project,category_name,country ,count(*) as add_click_num from dwd.dwd_fd_category_data_analyze_goods_event_detail
where project in ('floryday','airydress')
-- 加车事件
and event_name='add'
group by  project,category_name,country) t4
on t1.project=t4.project and t1.category_name=t4.category_name and t1.country=t4.country

left join
--商品的详情页曝光量
(select project,category_name,country ,count(*) as product_impression_num from dwd.dwd_fd_category_data_analyze_goods_event_detail
where project in ('floryday','airydress')
and page_code='product' and event_name='goods_impression'
group by  project,category_name,country) t7
on t1.project=t7.project and t1.category_name=t7.category_name and t1.country=t7.country


left join
-- 销量,销售额,订单量
 (select  project, category_name,country,
 sum(cast(goods_number as bigint)) as sales, --销量
 sum(cast(goods_number as bigint)*cast (shop_price as double)) as sales_volume,--销售额
 count(*) as order_valume --订单量
 from dwd.dwd_fd_category_data_analyze_order_detail
 where project in('floryday','airydress')
 group by project,category_name,country) t5
on t1.project=t5.project and t1.category_name=t5.category_name and t1.country=t5.country

left join
-- 连单的数量
(
-- 统计国家，品类，项目维度下的连单数量（单笔订单同一品类的商品数量大于2的订单）的订单个数
select t1.project,t1.category_name,t1.country, count(*) as link_order_num from (
-- 单笔订单同一品类的商品数量大于2的订单（订单号）
     select order_id,count(*),project,category_name,country from dwd.dwd_fd_category_data_analyze_order_detail
     group by order_id,project,category_name,country  having count(*) >=2 ) t1
group by t1.project,t1.category_name,t1.country ) t6
on t1.project=t6.project and t1.category_name=t6.category_name and t1.country=t6.country


union all
-- 所有国家的聚合结果
select
concat('${pt}',' 00:00:00') as reporte_date,
t1.project,
t1.category_name,
'all' as country,
t1.impression_num,
nvl(t2.advs_product_pv,0),
nvl(t3.click_num,0),
nvl(t3.click_num/t1.impression_num ,0)as ctr_rate,
nvl(t4.add_click_num/t7.product_impression_num ,0) as add_car_rate,
nvl(t5.sales_volume,0) as sales_volume,
nvl(t5.sales,0) as sales,
nvl(t5.order_valume,0) as order_numbers,
nvl(t5.sales_volume/t5.order_valume ,0)as avg_order_fees,
nvl(t6.link_order_num/t5.order_valume,0) as link_order_rate,
nvl(t5.order_valume/t1.impression_num ,0)as all_ctr_rate
from
----根据国家，品类，项目计算指定页面下的曝光量
(select project,category_name,count(*) as impression_num from dwd.dwd_fd_category_data_analyze_goods_event_detail
where project in ('floryday','airydress')
-- 指定的几个品类
and category_name in ('Dress','Blouses','Shoes','Coats','Swimwear','Sweaters','Pants&Leggings','Sweatshirts','T-shirts')
and event_name='goods_impression'
-- 指定页面的曝光
and (
(source_type='PC' and page_code in ('list','sale','search','finalSaleV2List','promotion','activityList','homepage'))
or
(source_type='APP' and page_code in ('list','sale','topPicks','searchResult','buywithpoints','activityList','homepage'))
or
(source_type='H5' and page_code in ('list','sale','hotClearance','promotion','homepage','activityList'))
)
group by  project,category_name) t1

left join
--广告商品详情页pv
(select project,category_name,count(*) as advs_product_pv from dwd.dwd_fd_category_data_analyze_goods_event_detail
where project in ('floryday','airydress') and mkt_source <> '' and page_code='product' and category_name is not null
group by  project,category_name) t2
on t1.project=t2.project and t1.category_name=t2.category_name

left join
--商品的所有点击量
(select project,category_name,count(*) as click_num from dwd.dwd_fd_category_data_analyze_goods_event_detail
where project in ('floryday','airydress')
and event_name='goods_click'
-- 指定页面的点击量
and (
(source_type='PC' and page_code in ('list','sale','search','finalSaleV2List','promotion','activityList','homepage'))
or
(source_type='APP' and page_code in ('list','sale','topPicks','searchResult','buywithpoints','activityList','homepage'))
or
(source_type='H5' and page_code in ('list','sale','hotClearance','promotion','homepage','activityList'))
)
group by  project,category_name) t3
on t1.project=t3.project and t1.category_name=t3.category_name

left join
--商品的加车点击量
(select project,category_name,count(*) as add_click_num from dwd.dwd_fd_category_data_analyze_goods_event_detail
where project in ('floryday','airydress')
and event_name='add'
and category_name is not null
group by  project,category_name) t4
on t1.project=t4.project and t1.category_name=t4.category_name

left join
--商品的详情页曝光量
(select project,category_name ,count(*) as product_impression_num from dwd.dwd_fd_category_data_analyze_goods_event_detail
where project in ('floryday','airydress')
and page_code='product' and event_name='goods_impression'
and category_name is not null
group by  project,category_name) t7
on t1.project=t7.project and t1.category_name=t7.category_name


left join
-- 销量,销售额,订单量
 (select  project, category_name,
 sum(cast(goods_number as bigint)) as sales, --销量
 sum(cast(goods_number as bigint)*cast (shop_price as double)) as sales_volume,--销售额
 count(*) as order_valume --订单量
 from dwd.dwd_fd_category_data_analyze_order_detail
 where project in('floryday','airydress')
 group by project,category_name) t5
on t1.project=t5.project and t1.category_name=t5.category_name

left join
-- 连单的数量
(
-- 统计国家，品类，项目维度下的连单数量（单笔订单同一品类的商品数量大于2的订单）的订单个数
select t1.project,t1.category_name, count(*) as link_order_num from (
-- 单笔订单同一品类的商品数量大于2的订单（订单号）
     select order_id,count(*),project,category_name from dwd.dwd_fd_category_data_analyze_order_detail
     group by order_id,project,category_name  having count(*) >=2 ) t1
group by t1.project,t1.category_name) t6
on t1.project=t6.project and t1.category_name=t6.category_name ;

