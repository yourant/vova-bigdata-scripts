#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pt=$(date -d "-1 day" +%Y-%m-%d)
fi
pre_m=`date -d "29 day ago ${pt}" +%Y-%m-%d`
echo "$pre_m"

sql="
--商品近一月表现
with  ads_goods_id_behave_m_ctr as
(
select
goods_id,
sum(clicks) clicks,
sum(impressions) impressions,
count(distinct clk_buyer_id) users,
count(distinct expre_buyer_id) impression_users,
nvl((sum(clicks)+0.015*50)/(sum(impressions)+50),0) ctr,
first(show_price) show_price,
first(shop_price) shop_price,
first(brand_id) brand_id
from
(
select
g.goods_id,
buyer_id,
clicks,
impressions,
case when event_name='goods_click' then buyer_id else null end clk_buyer_id,
case when event_name='goods_impression' then buyer_id else null end expre_buyer_id,
g.shop_price + g.shipping_fee show_price,
g.shop_price,
g.brand_id
from
(
select event_name,buyer_id,geo_country,virtual_goods_id,1 clicks,0 impressions from dwd.dwd_vova_log_goods_click where pt>='$pre_m' and pt<='$pt' and dp ='vova' and buyer_id>0  and platform ='mob'
union all
select event_name,buyer_id,geo_country,virtual_goods_id goods_id,0 clicks,1 impressions from dwd.dwd_vova_log_goods_impression where pt>='$pre_m' and pt<='$pt' and dp ='vova' and buyer_id>0 and platform ='mob'
) t join dim.dim_vova_goods g on g.virtual_goods_id = t.virtual_goods_id
) t1 group by goods_id
),

--商品近一月gmv
ads_goods_id_behave_m_gmv as
(
select
goods_id,
sum(shop_price * goods_number + shipping_fee)  gmv,
sum(goods_number)  sales_order,
count(distinct buyer_id) payed_user_num
from dwd.dwd_vova_fact_pay where to_date(pay_time)>='$pre_m'
group by goods_id
),
ads_goods_id_behave_m_cart as
(
select
g.goods_id,
count(buyer_id) cart_pv,
count(distinct buyer_id) cart_uv
from
(
select
cast(element_id as bigint) virtual_goods_id,
buyer_id
from dwd.dwd_vova_log_common_click
where pt>='$pre_m' and pt<='$pt'
and dp ='vova' and element_name='pdAddToCartSuccess' and platform ='mob'
) t join dim.dim_vova_goods g on g.virtual_goods_id = t.virtual_goods_id
group by g.goods_id
),
ads_goods_id_behave_m_sor as
(
select
goods_id,
(sum(so_order_cnt_3_6w)+0.74*10)/(count(order_goods_id)+10) sor
from
(
select
og.goods_id,
og.order_goods_id,
case when datediff(fl.valid_tracking_date,fl.confirm_time)<=7 then 1 else 0 end so_order_cnt_3_6w
from dim.dim_vova_order_goods og
left join dwd.dwd_vova_fact_logistics fl on fl.order_goods_id=og.order_goods_id
where datediff('$pt', date(og.confirm_time)) between 21 and 42
and og.sku_pay_status>1
and og.sku_shipping_status > 0
) t1 group by goods_id
),
ads_goods_id_behave_m_grr as
(
select
goods_id,
(sum(t1.nlrf_order_cnt_5_8w)+0.1*5)/(count(t1.order_goods_id)+5) grr
from
(
select
og.goods_id,
og.order_goods_id,
case when fr.refund_reason_type_id != 8 and fr.refund_type_id=2 then 1 else 0 end nlrf_order_cnt_5_8w
from dim.dim_vova_order_goods og
left join dwd.dwd_vova_fact_refund fr on fr.order_goods_id=og.order_goods_id
left join dwd.dwd_vova_fact_logistics fl on fr.order_goods_id=fl.order_goods_id
where datediff('$pt', date(og.confirm_time)) between 35 and 56
and og.sku_pay_status>1
and og.sku_shipping_status > 0
) t1 group by goods_id
),
ads_goods_id_behave_m_lgrr as
(
select
t1.goods_id,
(sum(t1.lrf_order_cnt_9_12w)+0.1*5)/(count(t1.order_goods_id)+5) lgrr
from
(
select
og.goods_id,
og.order_goods_id,
case when fr.refund_reason_type_id=8 and fr.refund_type_id=2 then 1 else 0 end lrf_order_cnt_9_12w
from  dim.dim_vova_order_goods og
left join dwd.dwd_vova_fact_refund fr on fr.order_goods_id=og.order_goods_id
left join dwd.dwd_vova_fact_logistics fl on fr.order_goods_id=fl.order_goods_id
where datediff('$pt', date(og.confirm_time)) between 63 and 84
and og.sku_pay_status>1
and og.sku_shipping_status > 0
) t1 group by goods_id
)
insert overwrite table ads.ads_vova_goods_id_behave_m partition(pt='${pt}')
select
ctr.goods_id,
clicks,
impressions,
nvl(sales_order,0) sales_order,
users,
impression_users,
nvl(payed_user_num,0) payed_user_num,
nvl(gmv,0) gmv,
nvl(ctr,0) ctr,
case when impressions<2000 then 0.003 else nvl((gmv/users)*ctr,0) end gcr,
nvl((payed_user_num+0.003*1000)/(impression_users+1000),0) cr,
nvl(payed_user_num /users,0) click_cr,
nvl(grr,0) grr,
nvl(sor,0) sor,
nvl(lgrr,0) lgrr,
nvl(sales_order/users,0) rate,
nvl(gmv/users*100,0) gr,
nvl(cart_uv,0) cart_uv,
nvl(cart_pv,0) cart_pv,
case when impression_users<100 then 0.002 else nvl((cart_uv+0.002*300)/(impression_users+300),0) end cart_rate,
nvl(shop_price,0) shop_price,
nvl(show_price,0) show_price,
nvl(brand_id,0) brand_id
from ads_goods_id_behave_m_ctr ctr
left join ads_goods_id_behave_m_gmv gmv on ctr.goods_id = gmv.goods_id
left join ads_goods_id_behave_m_cart cart on ctr.goods_id = cart.goods_id
left join ads_goods_id_behave_m_sor sor on ctr.goods_id = sor.goods_id
left join ads_goods_id_behave_m_grr grr on ctr.goods_id = grr.goods_id
left join ads_goods_id_behave_m_lgrr lgrr on ctr.goods_id = lgrr.goods_id;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql  --conf "spark.app.name=ads_vova_goods_id_behave_m_zhangyin" --conf "spark.dynamicAllocation.maxExecutors=150" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi