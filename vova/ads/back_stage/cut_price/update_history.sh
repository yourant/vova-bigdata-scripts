#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pre_date=`date -d "-1 day" +%Y-%m-%d`
fi
sql="
drop table if exists tmp.cut_price_goods_sn;
create table tmp.cut_price_goods_sn as
select
gc.goods_sn,
gc.clks,
gc.expres,
gc.expre_uv,
gc.clk_uv,
gc.ctr,
nvl(gg.gmv,0) gmv,
nvl(gg.gmv/gc.clk_uv * gc.ctr,0) gcr
from 
(
select
g.goods_sn,
sum(clicks) clks,
sum(impressions) expres,
count(distinct impression_device_id) expre_uv,
count(distinct click_device_id) clk_uv,
sum(clicks)/sum(impressions) ctr
from
(
select virtual_goods_id, device_id click_device_id,null impression_device_id,1 clicks,0 impressions from dwd.fact_log_goods_click where pt='$pre_date' and platform='mob' and datasource='vova'
union all
select virtual_goods_id, null click_device_id,device_id impression_device_id,0 clicks,1 impressions from dwd.fact_log_goods_impression where pt='$pre_date' and platform='mob' and datasource='vova'
) t1 join dwd.dim_goods g on t1.virtual_goods_id = g.virtual_goods_id
group by g.goods_sn
) gc
left join
(
select
g.goods_sn,
sum(p.shop_price*p.goods_number+p.shipping_fee) as gmv
from dwd.fact_pay p
left join dwd.dim_goods g on p.goods_id = g.goods_id
where to_date(pay_time)='$pre_date' and p.datasource ='vova' and platform in ('ios','android')
group by g.goods_sn
) gg on gc.goods_sn = gg.goods_sn;

insert overwrite table ads.ads_goods_sn_cut_price PARTITION (pt = '$pre_date')
select
/*+ REPARTITION(1) */
distinct event_date,goods_sn
from
(
select '$pre_date' event_date,goods_sn from (select goods_sn from tmp.cut_price_goods_sn order by gmv desc limit 100) t1
union all
select '$pre_date' event_date,goods_sn from (select goods_sn from tmp.cut_price_goods_sn where expres>1000 order by gcr desc limit 100) t2
) t;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql --conf "spark.app.name=ads_goods_sn_cut_price" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi