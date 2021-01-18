#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

###逻辑sql
#依赖的表，dim_category，dim_goods，fact_pay，fact_log_goods_click，fact_log_common_click，fact_comment，fact_refund
sql="
set hive.groupby.position.alias=false;
--step1 商品信息临时表
drop table if exists tmp.tmp_vova_fact_buyer_portrait_base_01;
create table tmp.tmp_vova_fact_buyer_portrait_base_01  STORED AS PARQUETFILE as
select /*+ REPARTITION(2) */ goods_id,virtual_goods_id,goods_name,first_cat_id from dim.dim_vova_goods;

--step2 购买行为1
drop table if exists tmp.tmp_vova_fact_buyer_portrait_base_buy;
create table tmp.tmp_vova_fact_buyer_portrait_base_buy  STORED AS PARQUETFILE as
select /*+ REPARTITION(1) */ t1.datasource,
t1.buyer_id,
t1.goods_id,
t2.goods_name,
sum(t1.goods_number) cnt,
t2.first_cat_id,1 act_type_id
from
(
select goods_id,goods_number,buyer_id,datasource from dwd.dwd_vova_fact_pay where to_date(order_time)='$pre_date'
) t1
left outer join
tmp.tmp_vova_fact_buyer_portrait_base_01 t2
on t1.goods_id = t2.goods_id group by t1.datasource,
t1.buyer_id,
t1.goods_id,
t2.goods_name,t2.first_cat_id;

--step3 浏览行为2
drop table if exists tmp.tmp_vova_fact_buyer_portrait_base_clk;
create table tmp.tmp_vova_fact_buyer_portrait_base_clk  STORED AS PARQUETFILE as
select t1.datasource,
t1.buyer_id,
t2.goods_id,
t2.goods_name,
count(1) cnt,
t2.first_cat_id,
2 act_type_id from
(
select datasource,virtual_goods_id,buyer_id from dwd.dwd_vova_log_goods_click where pt='$pre_date' and buyer_id<> -1 and buyer_id is not null
) t1
left outer join
tmp.tmp_vova_fact_buyer_portrait_base_01 t2
on t1.virtual_goods_id = t2.virtual_goods_id
group by t1.datasource,
t1.buyer_id,
t2.goods_id,
t2.goods_name,
t2.first_cat_id;

--step4 评论行为3
drop table if exists tmp.tmp_vova_fact_buyer_portrait_base_com;
create table tmp.tmp_vova_fact_buyer_portrait_base_com  STORED AS PARQUETFILE as
select /*+ REPARTITION(1) */ t1.datasource,t1.buyer_id,t1.goods_id,t2.goods_name,count(1) cnt,t2.first_cat_id,3 act_type_id from
(
select datasource,buyer_id,goods_id from dwd.dwd_vova_fact_comment where to_date(post_time) ='$pre_date'
) t1
left outer join
tmp.tmp_vova_fact_buyer_portrait_base_01 t2
on t1.goods_id = t2.goods_id
group by t1.datasource,t1.buyer_id,t1.goods_id,t2.goods_name,t2.first_cat_id;

--step5 收藏4,取消收藏5，加购6
drop table if exists tmp.tmp_vova_fact_buyer_portrait_base_cart;
create table tmp.tmp_vova_fact_buyer_portrait_base_cart  STORED AS PARQUETFILE as
select t1.datasource,t1.buyer_id,t2.goods_id,t2.goods_name,count(1) cnt,t2.first_cat_id,
case when t1.element_name ='pdAddToWishlistClick' then 4
when t1.element_name='pdRemoveFromWishlistClick' then 5
when t1.element_name in ('pdAddToCartSuccess','h5flashsaleSkuPopupGetitnowButton') then 6
end as act_type_id from
(
select datasource,cast(element_id as bigint) virtual_goods_id,buyer_id,element_name from dwd.dwd_vova_log_common_click
where pt='$pre_date' and buyer_id<> -1 and buyer_id is not null
and element_name in ('pdAddToWishlistClick','pdRemoveFromWishlistClick','pdAddToCartSuccess','h5flashsaleSkuPopupGetitnowButton')
and  element_id<>'' and element_id is not null
) t1
left outer join
tmp.tmp_vova_fact_buyer_portrait_base_01 t2
on t1.virtual_goods_id = t2.virtual_goods_id
group by t1.datasource,t1.buyer_id,t2.goods_id,t2.goods_name,t2.first_cat_id,
case when t1.element_name ='pdAddToWishlistClick' then 4
when t1.element_name='pdRemoveFromWishlistClick' then 5
when t1.element_name in ('pdAddToCartSuccess','h5flashsaleSkuPopupGetitnowButton') then 6
end;

--step6,8：语言
drop table if exists tmp.tmp_vova_fact_buyer_portrait_base_lan;
create table tmp.tmp_vova_fact_buyer_portrait_base_lan  STORED AS PARQUETFILE as
select distinct datasource,buyer_id,language,country,1 cnt,8 act_type_id from dwd.dwd_vova_log_screen_view
where pt='$pre_date' and buyer_id<> -1 and buyer_id is not null;
--step8,9:日活跃时段

--0-6:星期日-星期六，周活跃度
drop table if exists tmp.tmp_vova_fact_buyer_portrait_base_act_w;
create table tmp.tmp_vova_fact_buyer_portrait_base_act_w as
select datasource,buyer_id,tag_id,'active_week' tag_name,count(1) cnt,9 act_type_id from
(
select datasource,buyer_id,
pmod(datediff(from_unixtime(collector_tstamp,'yyyy-MM-dd'),'1920-01-01')-3,7) tag_id
from dwd.dwd_vova_log_goods_click where pt='$pre_date' and buyer_id<> -1 and buyer_id is not null
union all
select datasource,buyer_id,
pmod(datediff(from_unixtime(collector_tstamp,'yyyy-MM-dd'),'1920-01-01')-3,7) tag_id
from dwd.dwd_vova_log_common_click
where pt='$pre_date' and buyer_id<> -1 and buyer_id is not null
and element_name in ('pdAddToWishlistClick','pdAddToCartSuccess','h5flashsaleSkuPopupGetitnowButton')
and  element_id<>'' and element_id is not null
union all
select datasource,buyer_id,pmod(datediff(to_date(order_time),'1920-01-01')-3,7) tag_id from dwd.dwd_vova_fact_pay where to_date(order_time)='$pre_date'
) t group by datasource,buyer_id,tag_id,'active_week',null ;

--月活跃度
drop table if exists tmp.tmp_vova_fact_buyer_portrait_base_act_m;
create table tmp.tmp_vova_fact_buyer_portrait_base_act_m  STORED AS PARQUETFILE as
select datasource,buyer_id,tag_id,'active_month' tag_name,count(1) cnt,9 as act_type_id from
(
select datasource,buyer_id,
case when day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))>=1 and day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))<4 then '1'
when day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))>=4 and day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))<7 then '4'
when day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))>=7 and day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))<10 then '7'
when day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))>=10 and day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))<13 then '10'
when day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))>=13 and day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))<16 then '13'
when day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))>=16 and day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))<19 then '16'
when day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))>=19 and day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))<22 then '19'
when day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))>=22 and day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))<25 then '22'
when day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))>=25 and day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))<28 then '25'
when day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))>=28 then '28'
end as tag_id
from dwd.dwd_vova_log_goods_click where pt='$pre_date' and buyer_id<> -1 and buyer_id is not null
union all
select datasource,buyer_id,
case when day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))>=1 and day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))<4 then '1'
when day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))>=4 and day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))<7 then '4'
when day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))>=7 and day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))<10 then '7'
when day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))>=10 and day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))<13 then '10'
when day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))>=13 and day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))<16 then '13'
when day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))>=16 and day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))<19 then '16'
when day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))>=19 and day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))<22 then '19'
when day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))>=22 and day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))<25 then '22'
when day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))>=25 and day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))<28 then '25'
when day(from_unixtime(collector_tstamp,'yyyy-MM-dd'))>=28 then '28'
end as tag_id
from dwd.dwd_vova_log_common_click
where pt='$pre_date' and buyer_id<> -1 and buyer_id is not null
and element_name in ('pdAddToWishlistClick','pdAddToCartSuccess','h5flashsaleSkuPopupGetitnowButton')
and  element_id<>'' and element_id is not null
union all
select datasource,buyer_id,
case when day(order_time)>=1 and day(order_time)<4 then '1'
when day(order_time)>=4 and day(order_time)<7 then '4'
when day(order_time)>=7 and day(order_time)<10 then '7'
when day(order_time)>=10 and day(order_time)<13 then '10'
when day(order_time)>=13 and day(order_time)<16 then '13'
when day(order_time)>=16 and day(order_time)<19 then '16'
when day(order_time)>=19 and day(order_time)<22 then '19'
when day(order_time)>=22 and day(order_time)<25 then '22'
when day(order_time)>=25 and day(order_time)<28 then '25'
when day(order_time)>=28 then '28'
end as tag_id from dwd.dwd_vova_fact_pay where to_date(order_time)='$pre_date'
) t group by datasource,buyer_id,tag_id,'active_month',null ;

--step6 退款完成行为7
drop table if exists tmp.tmp_vova_fact_buyer_portrait_base_ref;
create table tmp.tmp_vova_fact_buyer_portrait_base_ref  STORED AS PARQUETFILE as
select
/*+ REPARTITION(1) */
t1.datasource,
t3.buyer_id,
t3.goods_id,
t4.goods_name,
sum(t3.goods_number) cnt,
t4.first_cat_id,
7 act_type_id
from dwd.dwd_vova_fact_refund t1
left join dwd.dwd_vova_fact_pay t3 on t1.order_goods_id = t3.order_goods_id
left join tmp.tmp_vova_fact_buyer_portrait_base_01 t4 on t3.goods_id = t4.goods_id
where to_date (t1.exec_refund_time) = '$pre_date'
group by t1.datasource,t3.buyer_id,t3.goods_id,t4.first_cat_id,t4.goods_name;

insert overwrite table dwd.dwd_vova_fact_buyer_portrait_base PARTITION (pt = '$pre_date')
select
/*+ REPARTITION(2) */
datasource,
buyer_id,
goods_id,
goods_name,
cnt,
first_cat_id,
act_type_id
from
(
select datasource,buyer_id,cast(goods_id as string) goods_id,goods_name,cnt,first_cat_id,act_type_id from tmp.tmp_vova_fact_buyer_portrait_base_buy
union all
select datasource,buyer_id,cast(goods_id as string) goods_id,goods_name,cnt,first_cat_id,act_type_id from tmp.tmp_vova_fact_buyer_portrait_base_clk
union all
select datasource,buyer_id,cast(goods_id as string) goods_id,goods_name,cnt,first_cat_id,act_type_id from tmp.tmp_vova_fact_buyer_portrait_base_com
union all
select datasource,buyer_id,cast(goods_id as string) goods_id,goods_name,cnt,first_cat_id,act_type_id from tmp.tmp_vova_fact_buyer_portrait_base_cart
union all
select datasource,buyer_id,language goods_id,country goods_name,cnt,null first_cat_id,act_type_id from tmp.tmp_vova_fact_buyer_portrait_base_lan
union all
select datasource,buyer_id,cast(tag_id as string) goods_id,tag_name goods_name,cnt,null first_cat_id,act_type_id from (select datasource,buyer_id,tag_id,'active_day' tag_name,count(1) cnt,9 act_type_id from
(
select datasource,buyer_id,
case when hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))>=0 and hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))<3 then '0'
when hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))>=3 and hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))<6 then '3'
when hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))>=6 and hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))<9 then '6'
when hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))>=9 and hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))<12 then '9'
when hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))>=12 and hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))<15 then '12'
when hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))>=15 and hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))<18 then '15'
when hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))>=18 and hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))<21 then '18'
when hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))>=21 and hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))<24 then '21'
end as tag_id
from dwd.dwd_vova_log_goods_click where pt='$pre_date' and buyer_id<> -1 and buyer_id is not null
union all
select datasource,buyer_id,
case when hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))>=0 and hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))<3 then '0'
when hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))>=3 and hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))<6 then '3'
when hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))>=6 and hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))<9 then '6'
when hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))>=9 and hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))<12 then '9'
when hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))>=12 and hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))<15 then '12'
when hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))>=15 and hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))<18 then '15'
when hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))>=18 and hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))<21 then '18'
when hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))>=21 and hour(from_unixtime(collector_tstamp,'yyyy-MM-dd HH:mm:ss'))<24 then '21'
end as tag_id
from dwd.dwd_vova_log_common_click
where pt='$pre_date' and buyer_id<> -1 and buyer_id is not null
and element_name in ('pdAddToWishlistClick','pdAddToCartSuccess','h5flashsaleSkuPopupGetitnowButton')
and  element_id<>'' and element_id is not null
union all
select datasource,buyer_id,
case when hour(order_time) >=0 and hour(order_time) <3 then '0'
when hour(order_time) >=3 and  hour(order_time) <6 then '3'
when hour(order_time) >=6 and  hour(order_time) <9 then '6'
when hour(order_time) >=9 and  hour(order_time) <12 then '9'
when hour(order_time) >=12 and hour(order_time) <15 then '12'
when hour(order_time) >=15 and hour(order_time) <18 then '15'
when hour(order_time) >=18 and hour(order_time) <21 then '18'
when hour(order_time) >=21 and hour(order_time) <24 then '21'
end as tag_id from dwd.dwd_vova_fact_pay where to_date(order_time)='$pre_date'
) t group by datasource,buyer_id,tag_id,'active_day',null) t
union all
select datasource,buyer_id,cast(tag_id as string) goods_id,tag_name goods_name,cnt,null first_cat_id,act_type_id from tmp.tmp_vova_fact_buyer_portrait_base_act_w
union all
select datasource,buyer_id,cast(tag_id as string) goods_id,tag_name goods_name,cnt,null first_cat_id,act_type_id from tmp.tmp_vova_fact_buyer_portrait_base_act_m
union all
select datasource,buyer_id,cast(goods_id as string) goods_id,goods_name,cnt,first_cat_id,act_type_id from tmp.tmp_vova_fact_buyer_portrait_base_ref
) t;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql --conf "spark.app.name=fact_buyer_portrait_base" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

