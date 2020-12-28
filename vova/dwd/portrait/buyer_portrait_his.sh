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
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=1000;
set hive.groupby.position.alias=false;

--step1 商品信息临时表
drop table if exists tmp.tmp_vova_fact_buyer_portrait_base_01;
create table tmp.tmp_vova_fact_buyer_portrait_base_01 as
select t1.goods_id,t1.virtual_goods_id,t1.goods_name,t2.first_cat_id from
(
select goods_id,virtual_goods_id,cat_id,goods_name from dim.dim_vova_goods
) t1
left outer join
dim.dim_vova_category t2
on t1.cat_id = t2.cat_id;

--step2 购买行为1
insert into table dwd.dwd_vova_fact_buyer_portrait_base  partition(pt)
select t1.datasource,
t1.buyer_id,
t1.goods_id,
t2.goods_name,
sum(t1.goods_number) cnt,
t2.first_cat_id,1 act_type_id,t1.pt pt
from
(
select goods_id,goods_number,buyer_id,datasource, to_date(order_time) pt from dwd.dwd_vova_fact_pay where to_date(order_time)>='$pre_date'
) t1
left outer join
tmp.tmp_vova_fact_buyer_portrait_base_01 t2
on t1.goods_id = t2.goods_id
group by t1.datasource,
t1.buyer_id,
t1.goods_id,
t2.goods_name,t2.first_cat_id,t1.pt;

--step3 浏览行为2
insert into table dwd.dwd_vova_fact_buyer_portrait_base  partition(pt)
select t1.datasource,
t1.buyer_id,
t2.goods_id,
t2.goods_name,
count(1) cnt,
t2.first_cat_id,
2 act_type_id,t1.pt pt from
(
select datasource,virtual_goods_id,buyer_id,pt from dwd.dwd_vova_fact_log_goods_click where pt>='$pre_date' and buyer_id<> -1 and buyer_id is not null
) t1
left outer join
tmp.tmp_vova_fact_buyer_portrait_base_01 t2
on t1.virtual_goods_id = t2.virtual_goods_id
group by t1.datasource,
t1.buyer_id,
t2.goods_id,
t2.goods_name,
t2.first_cat_id,t1.pt;

--step4 评论行为3
insert  into table dwd.dwd_vova_fact_buyer_portrait_base  partition(pt)
select t1.datasource,t1.buyer_id,t1.goods_id,t2.goods_name,count(1),t2.first_cat_id,3,t1.pt pt from
(
select datasource,buyer_id,goods_id,to_date(post_time) pt from dwd.dwd_vova_fact_comment where to_date(post_time) >='$pre_date'
) t1
left outer join
tmp.tmp_vova_fact_buyer_portrait_base_01 t2
on t1.goods_id = t2.goods_id
group by t1.datasource,t1.buyer_id,t1.goods_id,t2.goods_name,t2.first_cat_id,t1.pt;

--step5 收藏4,取消收藏5，加购6
insert into table dwd.dwd_vova_fact_buyer_portrait_base  partition(pt)
select t1.datasource,t1.buyer_id,t2.goods_id,t2.goods_name,count(1) cnt,t2.first_cat_id,
case when t1.element_name ='pdAddToWishlistClick' then 4
when t1.element_name='pdRemoveFromWishlistClick' then 5
when t1.element_name in ('pdAddToCartSuccess','h5flashsaleSkuPopupGetitnowButton') then 6
end as act_type_id,t1.pt pt from
(
select datasource,cast(element_id as bigint) virtual_goods_id,buyer_id,element_name,pt from dwd.dwd_vova_fact_log_common_click
where pt>='$pre_date' and buyer_id<> -1 and buyer_id is not null
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
end,t1.pt;

--step6,8：语言
insert into table dwd.dwd_vova_fact_buyer_portrait_base  partition(pt)
select distinct datasource,buyer_id,language,country,1,null,8,pt from dwd.dwd_vova_fact_log_screen_view
where pt>='$pre_date' and buyer_id<> -1 and buyer_id is not null;
--step8,9:日活跃时段
insert into table dwd.dwd_vova_fact_buyer_portrait_base  partition(pt)
select datasource,buyer_id,tag_id,'active_day' tag_name,count(1),null,9,pt from
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
end as tag_id,pt
from dwd.dwd_vova_fact_log_goods_click where pt>='$pre_date' and buyer_id<> -1 and buyer_id is not null
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
end as tag_id,pt
from dwd.dwd_vova_fact_log_common_click
where pt>='$pre_date' and buyer_id<> -1 and buyer_id is not null
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
end as tag_id,cast(to_date(order_time) as string) pt from dwd.dwd_vova_fact_pay where to_date(order_time)>='$pre_date'
) t group by datasource,buyer_id,tag_id,'active_day',null,pt;
--0-6:星期日-星期六，周活跃度
insert into table dwd.dwd_vova_fact_buyer_portrait_base  partition(pt)
select datasource,buyer_id,tag_id,'active_week' tag_name,count(1),null,9,pt from
(
select datasource,buyer_id,
pmod(datediff(from_unixtime(collector_tstamp,'yyyy-MM-dd'),'1920-01-01')-3,7) tag_id,pt
from dwd.dwd_vova_fact_log_goods_click where pt>='$pre_date' and buyer_id<> -1 and buyer_id is not null
union all
select datasource,buyer_id,
pmod(datediff(from_unixtime(collector_tstamp,'yyyy-MM-dd'),'1920-01-01')-3,7) tag_id,pt
from dwd.dwd_vova_fact_log_common_click
where pt>='$pre_date' and buyer_id<> -1 and buyer_id is not null
and element_name in ('pdAddToWishlistClick','pdAddToCartSuccess','h5flashsaleSkuPopupGetitnowButton')
and  element_id<>'' and element_id is not null
union all
select datasource,buyer_id,pmod(datediff(to_date(order_time),'1920-01-01')-3,7) tag_id,cast(to_date(order_time) as string) pt from dwd.dwd_vova_fact_pay where to_date(order_time)>='$pre_date'
) t group by datasource,buyer_id,tag_id,'active_week',null ,pt;

--月活跃度
insert into table dwd.dwd_vova_fact_buyer_portrait_base partition(pt)
select datasource,buyer_id,tag_id,'active_month' tag_name,count(1),null,9 as act_type_id,pt from
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
end as tag_id,pt
from dwd.dwd_vova_fact_log_goods_click where pt>='$pre_date' and buyer_id<> -1 and buyer_id is not null
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
end as tag_id,pt
from dwd.dwd_vova_fact_log_common_click
where pt>='$pre_date' and buyer_id<> -1 and buyer_id is not null
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
end as tag_id ,to_date(order_time) pt
from dwd.dwd_vova_fact_pay where cast(to_date(order_time) as string) >='$pre_date'
) t group by datasource,buyer_id,tag_id,'active_month',null,pt ;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql --conf "spark.app.name=dwd_vova_fact_buyer_portrait_base" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

hivesql="
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=1000;
set hive.groupby.position.alias=false;
insert into table  dwd.dwd_vova_fact_buyer_portrait_base PARTITION(pt)
select t2.datasource,t2.buyer_id,t2.goods_id,t4.goods_name,t2.cnt,t4.first_cat_id,7 act_type_id,t2.pt as pt  from
(
select t1.datasource,t3.buyer_id,t1.pt,t3.goods_id,sum(t3.goods_number) cnt   from
(
select datasource,order_goods_id,to_date(exec_refund_time) pt from dwd.fact_refund where to_date(exec_refund_time)>='$pre_date'
) t1
join
dwd.dwd_vova_fact_pay t3
on t1.order_goods_id=t3.order_goods_id
group by t1.datasource,t3.buyer_id,t3.goods_id,t1.pt
) t2
left outer join
tmp.tmp_vova_fact_buyer_portrait_base_01 t4
on t2.goods_id=t4.goods_id;
"
hive -e $hivesql
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
