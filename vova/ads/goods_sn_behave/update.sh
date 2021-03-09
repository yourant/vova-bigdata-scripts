#!/bin/bash
#指定日期和引擎
stime=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  stime=`date -d "-168 hour" "+%Y-%m-%d %H:00:00"`
fi
echo "$stime"
#默认小时
pre_pt=`date -d "${stime}" +%Y-%m-%d`
echo "$pre_pt"
etime=$2
if [ ! -n "$1" ]; then
  etime=`date -d "0 hour" "+%Y-%m-%d %H:00:00"`
fi
echo "etime=$etime"
pt=`date -d "$etime" +%Y-%m-%d`
echo "pt=$pt"

sql="
with ads_goods_sn_behave_ctr as (
select
goods_sn,
sum(clicks) clicks,
sum(impressions) impressions,
count(distinct clk_buyer_id) users,
count(distinct expre_buyer_id) impression_users,
min(show_price) show_price,
min(shop_price) shop_price,
min(brand_id) brand_id,
nvl((sum(clicks)+0.015*50)/(sum(impressions)+50),0) ctr
from
(
select
g.goods_id,
g.goods_sn,
buyer_id,
clicks,
impressions,
g.shop_price + g.shipping_fee show_price,
g.shop_price,
g.brand_id,
case when event_name='goods_click' then buyer_id else null end clk_buyer_id,
case when event_name='goods_impression' then buyer_id else null end expre_buyer_id
from
(
select event_name,buyer_id,gender,page_code,list_type,virtual_goods_id,1 clicks,0 impressions from dwd.dwd_vova_log_goods_click_arc
where pt>='$pre_pt' and pt<='$pt' and collector_ts>='$stime' and collector_ts<='$etime' and datasource ='vova' and buyer_id>0 and platform ='mob'
union all
select 'goods_click' event_name,buyer_id,gender,page_code,list_type,cast(element_id as bigint) virtual_goods_id,1 clicks,0 impressions from dwd.dwd_vova_log_click_arc
where pt>='$pre_pt' and pt<='$pt' and collector_ts>='$stime' and collector_ts<='$etime' and datasource ='vova' and event_type='goods' and buyer_id>0 and platform ='mob'
union all
select event_name,buyer_id,gender,page_code,list_type,virtual_goods_id goods_id,0 clicks,1 impressions from dwd.dwd_vova_log_goods_impression_arc
where pt>='$pre_pt' and pt<='$pt' and collector_ts>='$stime' and collector_ts<='$etime' and datasource ='vova' and buyer_id>0 and platform ='mob'
union all
select 'goods_impression' event_name,buyer_id,gender,page_code,list_type,cast(element_id as bigint) virtual_goods_id,0 clicks,1 impressions from dwd.dwd_vova_log_impressions_arc
where pt>='$pre_pt' and pt<='$pt'and collector_ts>='$stime' and collector_ts<='$etime' and datasource ='vova' and event_type='goods' and buyer_id>0 and platform ='mob'
) t join dim.dim_vova_goods g on g.virtual_goods_id = t.virtual_goods_id
) t1 group by goods_sn
),

--商品sn近七日gmv
ads_goods_sn_behave_gmv as (
select
goods_sn,
sum(shop_price * goods_number + shipping_fee)  gmv,
sum(goods_number)  sales_order,
count(distinct buyer_id) payed_user_num
from dwd.dwd_vova_fact_pay where to_date(pay_time)>='$pre_pt'
group by goods_sn
),
--商品sn近七日加车
ads_goods_sn_behave_cart as (
select
g.goods_sn,
count(buyer_id) cart_pv,
count(distinct buyer_id) cart_uv
from
(
select
cast(element_id as bigint) virtual_goods_id,
buyer_id
from dwd.dwd_vova_log_common_click_arc
where pt>='$pre_pt' and pt<='$pt' and collector_ts>='$stime' and collector_ts<='$etime'
and datasource ='vova' and element_name='pdAddToCartSuccess'
union all
select
cast(element_id as bigint) virtual_goods_id,
buyer_id
from dwd.dwd_vova_log_click_arc
where pt>='$pre_pt' and pt<='$pt' and collector_ts>='$stime' and collector_ts<='$etime'
and datasource ='vova' and element_name='pdAddToCartSuccess' and event_type='normal'
union all
select
cast(element_id as bigint) virtual_goods_id,
buyer_id
from dwd.dwd_vova_log_data_arc
where pt>='$pre_pt' and pt<='$pt' and collector_ts>='$stime' and collector_ts<='$etime'
and datasource ='vova' and element_name='pdAddToCartSuccess'
) t join dim.dim_vova_goods g on g.virtual_goods_id = t.virtual_goods_id
group by g.goods_sn
)
insert overwrite table ads.ads_vova_goods_sn_behave partition(pt='${pt}')
select
/*+ REPARTITION(10) */
ctr.goods_sn,
clicks,
impressions,
nvl(sales_order,0) sales_order,
users,
impression_users,
nvl(payed_user_num,0) payed_user_num,
nvl(gmv,0) gmv,
nvl(ctr,0) ctr,
case when sales_order<10 or impressions<100 then 0.003 else nvl((gmv/users)*ctr,0) end gcr,
nvl((payed_user_num+0.003*1000)/(impression_users+1000),0) cr,
nvl(payed_user_num /users,0) click_cr,
nvl(sales_order/users,0) rate,
nvl(gmv/users*100,0) gr,
nvl(cart_uv,0) cart_uv,
nvl(cart_pv,0) cart_pv,
case when impression_users<100 then 0.002 else nvl((cart_uv+0.002*300)/(impression_users+300),0) end cart_rate,
nvl(shop_price,0) shop_price,
nvl(show_price,0) show_price,
nvl(brand_id,0) brand_id
from ads_goods_sn_behave_ctr ctr
left join ads_goods_sn_behave_gmv gmv on ctr.goods_sn = gmv.goods_sn
left join ads_goods_sn_behave_cart cart on ctr.goods_sn = cart.goods_sn;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql  --conf "spark.app.name=ads_vova_goods_sn_behave_zhangyin" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf spark.executor.memory=6G  -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi