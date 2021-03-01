#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "0 day" +%Y-%m-%d`
echo "cur_date=${cur_date}"
fi
pt=`date -d "1 day ago ${cur_date}" +%Y-%m-%d`
echo "pt=${pt}"
pre_week=`date -d "7 day ago ${cur_date}" +%Y-%m-%d`
echo "pre_week=${pre_week}"
pre_month=`date -d "30 day ago ${cur_date}" +%Y-%m-%d`
echo "pre_month=${pre_month}"
pre_2_month=`date -d "60 day ago ${cur_date}" +%Y-%m-%d`
echo "pre_2_month=${pre_2_month}"
pre_6_month=`date -d "180 day ago ${cur_date}" +%Y-%m-%d`
echo "pre_6_month=${pre_6_month}"

sql="
insert overwrite table tmp.tmp_ads_vova_buyer_goods_rating_expre
select
buyer_id,
virtual_goods_id,
sum(expre_rating) expre_rating,
0 clk_rating,
0 cart_rating,
0 wish_rating,
0 order_rating
from
(
select
buyer_id,
virtual_goods_id,
0.5*exp(-0.4*datediff('$cur_date',pt)) expre_rating
from dwd.dwd_vova_log_goods_impression_arc
where pt>='$pre_week' and datasource ='vova' and buyer_id>0 and virtual_goods_id>0
union all
select
buyer_id,
cast(element_id as bigint) virtual_goods_id,
0.5*exp(-0.4*datediff('$cur_date',pt)) expre_rating
from dwd.dwd_vova_log_impressions_arc
where pt>='$pre_week' and datasource ='vova' and event_type='goods' and buyer_id>0 and cast(element_id as bigint)>0
) t group by buyer_id,virtual_goods_id;

insert overwrite table tmp.tmp_ads_vova_buyer_goods_rating_clk
select
buyer_id,
virtual_goods_id,
0 expre_rating,
sum(clk_rating) clk_rating,
0 cart_rating,
0 wish_rating,
0 order_rating
from
(
select
buyer_id,
virtual_goods_id,
exp(-0.15*datediff('$cur_date',pt)) clk_rating
from dwd.dwd_vova_log_goods_click_arc
where pt>='$pre_month' and datasource ='vova' and buyer_id>0 and virtual_goods_id>0
union all
select
buyer_id,
cast(element_id as bigint) virtual_goods_id,
exp(-0.15*datediff('$cur_date',pt)) clk_rating
from dwd.dwd_vova_log_click_arc
where pt>='$pre_month' and datasource ='vova' and event_type='goods' and buyer_id>0 and cast(element_id as bigint)>0
) t group by buyer_id,virtual_goods_id;

insert overwrite table tmp.tmp_ads_vova_buyer_goods_rating_cart
select
buyer_id,
virtual_goods_id,
0 expre_rating,
0 clk_rating,
sum(cart_rating) cart_rating,
0 wish_rating,
0 order_rating
from
(
select
buyer_id,
cast(element_id as bigint) virtual_goods_id,
exp(-0.1*datediff('$cur_date',pt)) cart_rating
from dwd.dwd_vova_log_common_click_arc
where pt>='$pre_2_month' and datasource ='vova' and element_name='pdAddToCartSuccess'and buyer_id>0 and cast(element_id as bigint)>0
union all
select
buyer_id,
cast(element_id as bigint) virtual_goods_id,
exp(-0.1*datediff('$cur_date',pt)) cart_rating
from dwd.dwd_vova_log_click_arc
where pt>='$pre_2_month' and datasource ='vova' and element_name='pdAddToCartSuccess' and event_type='normal' and buyer_id>0 and cast(element_id as bigint)>0
union all
select
buyer_id,
cast(element_id as bigint) virtual_goods_id,
exp(-0.1*datediff('$cur_date',pt)) cart_rating
from dwd.dwd_vova_log_data_arc
where pt>='$pre_2_month' and datasource ='vova' and element_name='pdAddToCartSuccess' and buyer_id>0 and cast(element_id as bigint)>0
) t group by buyer_id,virtual_goods_id;

insert overwrite table tmp.tmp_ads_vova_buyer_goods_rating_wish
select
buyer_id,
virtual_goods_id,
0 expre_rating,
0 clk_rating,
0 cart_rating,
sum(wish_rating) wish_rating,
0 order_rating
from
(
select
buyer_id,
cast(element_id as bigint) virtual_goods_id,
exp(-0.1*datediff('$cur_date',pt)) wish_rating
from dwd.dwd_vova_log_common_click_arc
where pt>='$pre_2_month' and datasource ='vova' and element_name in ('pdAddToWishlistClick','addWishlist') and buyer_id>0 and cast(element_id as bigint)>0
union all
select
buyer_id,
cast(element_id as bigint) virtual_goods_id,
exp(-0.1*datediff('$cur_date',pt)) wish_rating
from dwd.dwd_vova_log_click_arc
where pt>='$pre_2_month' and datasource ='vova' and element_name in ('pdAddToWishlistClick','addWishlist') and event_type='normal' and buyer_id>0 and cast(element_id as bigint)>0
) t group by buyer_id,virtual_goods_id;

insert overwrite table tmp.tmp_ads_vova_buyer_goods_rating_order
select
buyer_id,
virtual_goods_id,
0 expre_rating,
0 clk_rating,
0 cart_rating,
0 wish_rating,
sum(order_rating) order_rating
from
(
select
buyer_id,
virtual_goods_id,
2*exp(-0.05*datediff('$cur_date',to_date(order_time))) order_rating
from dim.dim_vova_order_goods
where to_date(order_time)>='$pre_6_month' and datasource ='vova'
) t group by buyer_id,virtual_goods_id;

insert overwrite table ads.ads_vova_buyer_goods_rating PARTITION (pt = '${pt}')
select
/*+ REPARTITION(100) */
t.buyer_id user_id,
g.goods_id,
sum(1+clk_rating+cart_rating+wish_rating+order_rating-expre_rating) rating
from
(
select buyer_id,virtual_goods_id,expre_rating,clk_rating,cart_rating, wish_rating,order_rating from tmp.tmp_ads_vova_buyer_goods_rating_expre
union all
select buyer_id,virtual_goods_id,expre_rating,clk_rating,cart_rating, wish_rating,order_rating from tmp.tmp_ads_vova_buyer_goods_rating_clk
union all
select buyer_id,virtual_goods_id,expre_rating,clk_rating,cart_rating, wish_rating,order_rating from tmp.tmp_ads_vova_buyer_goods_rating_cart
union all
select buyer_id,virtual_goods_id,expre_rating,clk_rating,cart_rating, wish_rating,order_rating from tmp.tmp_ads_vova_buyer_goods_rating_wish
union all
select buyer_id,virtual_goods_id,expre_rating,clk_rating,cart_rating, wish_rating,order_rating from tmp.tmp_ads_vova_buyer_goods_rating_order
) t
left join dim.dim_vova_goods g on t.virtual_goods_id =g.virtual_goods_id
where g.goods_id>0  group by t.buyer_id,g.goods_id;
"
spark-sql --conf "spark.app.name=ads_vova_buyer_goods_rating_zhangyin" --conf "spark.dynamicAllocation.maxExecutors=150" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi