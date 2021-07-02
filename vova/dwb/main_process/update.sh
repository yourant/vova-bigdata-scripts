#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

sql="
insert overwrite table dwb.dwb_vova_main_process  PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
to_date('${cur_date}') as event_date,
t3.datasource,
t3.country,
t3.os_type,
t3.main_channel,
t3.is_new,
t3.dau,
t3.homepage_dau,
t3.pd_uv,
t3.pd_pv,
t3.pd_to_cart_uv,
t3.cart_uv,
t3.cart_to_checkout_uv,
t3.checkout_uv,
t3.pd_buy_uv,
t3.cart_success_uv,
t3.cart_success_pv,
t3.cart_buy_confirm_uv,
t3.checkout_place_order_uv,
t3.checkout_credit_card_uv,
nvl(t3.first_order_num,0) first_order_num,
nvl(t4.payed_user_num,0) payed_user_num,
nvl(t4.payed_order_num,0) payed_order_num,
nvl(t4.gmv,0) gmv,
nvl(t5.ordered_user_num,0) ordered_user_num,
nvl(t5.ordered_order_num,0) ordered_order_num,
nvl(t6.inac_payed_user_num,0) inac_payed_user_num,
nvl(t6.inac_payed_order_num,0) inac_payed_order_num,
nvl(t6.inac_gmv,0) inac_gmv,
nvl(t6.first_inac_payed_order_num,0) first_inac_payed_order_num,
nvl(t7.lck_gmv,0) lck_gmv,
'all' AS is_brand,
nvl(t4.brand_gmv,0) brand_gmv,
nvl(t4.no_brand_gmv,0) no_brand_gmv
from
(
select
nvl(t2.datasource,'all') datasource,
nvl(t2.geo_country,'all') country,
nvl(t2.os_type,'all') os_type,
nvl(t2.main_channel,'all') main_channel,
nvl(t2.is_new,'all') is_new,
count(distinct t2.device_id) as dau,
count(distinct t2.homepage_device_id) as homepage_dau,
count(distinct t2.pd_device_id) as pd_uv,
count(t2.pd_device_id) as pd_pv,
count(distinct t2.pd_to_cart_device_id) as pd_to_cart_uv,
count(distinct t2.cart_device_id) as cart_uv,
count(distinct t2.cart_to_checkout_device_id) as cart_to_checkout_uv,
count(distinct t2.checkout_device_id) as checkout_uv,
count(distinct t2.pdbuy_device_id) as pd_buy_uv,
count(distinct t2.cart_success_device_id) as cart_success_uv,
count(t2.cart_success_device_id) as cart_success_pv,
count(distinct t2.cart_buy_confirm_device_id) as cart_buy_confirm_uv,
count(distinct t2.checkout_place_order_device_id) as checkout_place_order_uv,
count(distinct t2.checkout_credit_card_device_id) as checkout_credit_card_uv,
count(distinct t2.first_order_id) as first_order_num
from
(
select
nvl(t1.datasource,'NA') datasource,
nvl(t1.geo_country,'NA') geo_country,
nvl(t1.os_type,'NA') os_type,
nvl(dd.main_channel,'NA') main_channel,
CASE WHEN datediff(t1.pt,dd.activate_time)<=0 THEN 'new'
  WHEN datediff(t1.pt,dd.activate_time)>=1 and datediff(t1.pt,dd.activate_time)<6 THEN '2-7'
  WHEN datediff(t1.pt,dd.activate_time)>=7 and datediff(t1.pt,dd.activate_time)<29 THEN '8-30'
  else '30+' END is_new,
CASE WHEN t1.event_name ='screen_view' THEN t1.device_id end device_id,
CASE when datediff(t1.pt,dd.first_pay_time)=0 THEN dd.first_order_id END first_order_id,
CASE when t1.page_code='homepage' and t1.event_name ='screen_view' THEN t1.device_id end homepage_device_id,
CASE when t1.page_code='product_detail' and t1.view_type='show' and t1.event_name ='screen_view' THEN t1.device_id end pd_device_id,
CASE when t1.page_code='cart' and t1.referrer like '%product_detail%' and t1.view_type='show' and t1.event_name ='screen_view' THEN t1.device_id end pd_to_cart_device_id,
CASE when t1.page_code='cart' and t1.view_type='show' and t1.event_name ='screen_view' THEN t1.device_id end cart_device_id,
CASE when t1.page_code like '%checkout%' and t1.view_type='show' and t1.referrer like '%cart%' and t1.event_name ='screen_view' THEN t1.device_id end cart_to_checkout_device_id,
CASE when t1.page_code like '%checkout%' and t1.view_type='show' and t1.event_name ='screen_view' THEN t1.device_id  end checkout_device_id,
CASE when t1.element_name='pdAddToCartClick' and t1.event_name ='common_click' THEN t1.device_id end pdbuy_device_id,
CASE when t1.page_code='product_detail' and t1.element_name='pdAddToCartSuccess' and t1.event_name ='common_click' THEN t1.device_id end cart_success_device_id,
CASE when t1.element_name='button_cart_checkout' and t1.event_name ='order_process' THEN t1.device_id end cart_buy_confirm_device_id,
CASE when t1.element_name='button_checkout_placeOrder' and t1.event_name ='order_process' THEN t1.device_id end checkout_place_order_device_id,
CASE WHEN t1.element_name='noCreditCardPayClick' and t1.event_name ='common_click' and page_code='checkout_credit_card' THEN t1.device_id end checkout_credit_card_device_id
from
(
select pt,datasource,event_name,geo_country,os_type,page_code,device_id,referrer,view_type,NULL element_name,NULL list_name from dwd.dwd_vova_log_screen_view where pt='${cur_date}' and platform ='mob' and os_type is not null and os_type !='' and device_id is not null
union all
select pt,datasource,event_name,geo_country,os_type,page_code,device_id,referrer,NULL view_type,element_name,list_name from dwd.dwd_vova_log_common_click where pt='${cur_date}' and platform ='mob' and os_type is not null and os_type !='' and device_id is not null
union all
select pt,datasource,event_name,geo_country,os_type,page_code,device_id,referrer,NULL view_type,element_name,NULL list_name from dwd.dwd_vova_log_order_process where pt='${cur_date}' and platform ='mob' and os_type is not null and os_type !='' and device_id is not null
) t1
left join dim.dim_vova_devices dd on dd.device_id = t1.device_id and dd.datasource=t1.datasource
) t2
group by
   t2.datasource,
   t2.geo_country,
   t2.os_type,
   t2.main_channel,
   t2.is_new
  with cube
) t3 left join
(
select
nvl(t1.datasource,'all')  datasource,
nvl(t1.region_code,'all') region_code,
nvl(t1.platform,'all') platform,
nvl(t1.main_channel,'all') main_channel,
nvl(t1.is_new,'all') is_new,
count(distinct t1.buyer_id) as payed_user_num,
count(distinct t1.order_id) as payed_order_num,
sum(t1.shop_price * t1.goods_number + t1.shipping_fee) as gmv,
sum(brand_gmv) brand_gmv,
sum(no_brand_gmv) no_brand_gmv
from
(
select
nvl(fp.datasource,'NA') datasource,
nvl(fp.region_code,'NA') region_code,
nvl(fp.platform,'NA') platform,
nvl(dd.main_channel,'NA') main_channel,
CASE WHEN datediff(fp.pay_time,dd.activate_time)<=0 THEN 'new'
  WHEN datediff(fp.pay_time,dd.activate_time)>=1 and datediff(fp.pay_time,dd.activate_time)<6 THEN '2-7'
  WHEN datediff(fp.pay_time,dd.activate_time)>=7 and datediff(fp.pay_time,dd.activate_time)<29 THEN '8-30'
  else '30+' END is_new,
fp.order_id,
fp.buyer_id,
fp.device_id,
fp.shop_price,
fp.shipping_fee,
fp.goods_number,
case when ddog.brand_id>0 then fp.shop_price * fp.goods_number + fp.shipping_fee else 0 end brand_gmv,
case when ddog.brand_id=0 then fp.shop_price * fp.goods_number + fp.shipping_fee else 0 end no_brand_gmv
from
dwd.dwd_vova_fact_pay fp
left join dim.dim_vova_devices dd on dd.device_id = fp.device_id and dd.datasource=fp.datasource
inner join dim.dim_vova_order_goods ddog on ddog.order_goods_id = fp.order_goods_id
where to_date(fp.pay_time)='${cur_date}' and fp.from_domain like '%api%'
and (ddog.order_tag not like '%luckystar_activity_id%' or ddog.order_tag is null)
) t1
group by
   t1.datasource,
   t1.region_code,
   t1.platform,
   t1.main_channel,
   t1.is_new
  with cube
)t4 on t3.datasource=t4.datasource and t3.country=t4.region_code and t3.os_type=t4.platform and t3.main_channel=t4.main_channel and t3.is_new=t4.is_new
left join
(
select
nvl(t1.datasource,'all') datasource,
nvl(t1.region_code,'all') region_code,
nvl(t1.platform,'all') platform,
nvl(t1.main_channel,'all') main_channel,
nvl(t1.is_new,'all') is_new,
count(distinct t1.buyer_id) as ordered_user_num,
count(distinct t1.order_id) as ordered_order_num
from
(
select
nvl(ddog.datasource,'NA') datasource,
nvl(ddog.region_code,'NA') region_code,
nvl(ddog.platform,'NA') platform,
nvl(dd.main_channel,'null') main_channel,
CASE WHEN datediff(ddog.order_time,dd.activate_time)<=0 THEN 'new'
  WHEN datediff(ddog.order_time,dd.activate_time)>=1 and datediff(ddog.order_time,dd.activate_time)<6 THEN '2-7'
  WHEN datediff(ddog.order_time,dd.activate_time)>=7 and datediff(ddog.order_time,dd.activate_time)<29 THEN '8-30'
  else '30+' END is_new,
ddog.order_id,
ddog.buyer_id,
ddog.device_id
from dim.dim_vova_order_goods ddog
left join dim.dim_vova_devices dd on dd.device_id = ddog.device_id and dd.datasource=ddog.datasource
where to_date(ddog.order_time) = '${cur_date}' and ddog.from_domain like '%api%'
and (ddog.order_tag not like '%luckystar_activity_id%' or ddog.order_tag is null)
) t1
group by
   t1.datasource,
   t1.region_code,
   t1.platform,
   t1.main_channel,
   t1.is_new
  with cube
) t5 on t3.datasource=t5.datasource and t3.country=t5.region_code and t3.os_type=t5.platform and t3.main_channel=t5.main_channel and t3.is_new=t5.is_new
left join
(
select
nvl(t1.datasource,'all')  datasource,
nvl(t1.region_code,'all') region_code,
nvl(t1.platform,'all') platform,
nvl(t1.main_channel,'all') main_channel,
nvl(t1.is_new,'all') is_new,
count(distinct t1.buyer_id) as inac_payed_user_num,
count(distinct t1.order_id) as inac_payed_order_num,
sum(t1.shop_price * t1.goods_number + t1.shipping_fee) as inac_gmv,
count(distinct t1.first_order_id) as first_inac_payed_order_num
from
(
select
nvl(fp.datasource,'NA') datasource,
nvl(fp.region_code,'NA') region_code,
nvl(fp.platform,'NA') platform,
nvl(dd.main_channel,'NA') main_channel,
CASE WHEN datediff(fp.pay_time,dd.activate_time)<=0 THEN 'new'
  WHEN datediff(fp.pay_time,dd.activate_time)>=1 and datediff(fp.pay_time,dd.activate_time)<6 THEN '2-7'
  WHEN datediff(fp.pay_time,dd.activate_time)>=7 and datediff(fp.pay_time,dd.activate_time)<29 THEN '8-30'
  else '30+' END is_new,
fp.order_id,
fp.buyer_id,
fp.device_id,
fp.shop_price,
fp.shipping_fee,
fp.goods_number,
CASE when datediff(fp.pay_time,dd.first_pay_time)=0 THEN dd.first_order_id END first_order_id
from
dwd.dwd_vova_fact_pay fp
left join dim.dim_vova_devices dd on dd.device_id = fp.device_id and dd.datasource=fp.datasource
inner join dim.dim_vova_order_goods ddog on ddog.order_goods_id = fp.order_goods_id
where to_date(fp.pay_time)='${cur_date}'and fp.from_domain like '%api%'
and ddog.order_tag is null
) t1
group by
   t1.datasource,
   t1.region_code,
   t1.platform,
   t1.main_channel,
   t1.is_new
  with cube
) t6 on t3.datasource=t6.datasource and t3.country=t6.region_code and t3.os_type=t6.platform and t3.main_channel=t6.main_channel and t3.is_new=t6.is_new
left join
(
select
nvl(t1.datasource,'all') datasource,
nvl(t1.region_code,'all') region_code,
nvl(t1.platform,'all') platform,
nvl(t1.main_channel,'all') main_channel,
nvl(t1.is_new,'all') is_new,
sum(lck_gmv) as lck_gmv
from
(
select
nvl(fag.datasource,'NA') datasource,
nvl(fag.rgn_code,'NA') region_code,
nvl(fag.platform,'NA') platform,
nvl(dd.main_channel,'NA') main_channel,
CASE WHEN datediff(fag.pay_time,dd.activate_time)<=0 THEN 'new'
  WHEN datediff(fag.pay_time,dd.activate_time)>=1 and datediff(fag.pay_time,dd.activate_time)<6 THEN '2-7'
  WHEN datediff(fag.pay_time,dd.activate_time)>=7 and datediff(fag.pay_time,dd.activate_time)<29 THEN '8-30'
  else '30+' END is_new,
fag.ord_amt + fag.ship_fee as lck_gmv
from
dwd.dwd_vova_fact_act_ord_gs fag
left join dwd.dwd_vova_fact_buyer_device_releation fbr on fag.byr_id = fbr.buyer_id and fag.datasource = fbr.datasource
left join dim.dim_vova_devices dd on dd.device_id = fbr.device_id and dd.datasource=fbr.datasource
where to_date(fag.pay_time)='${cur_date}' and fbr.pt='${cur_date}' and fag.pay_sts>=1
) t1
group by
t1.datasource,
t1.region_code,
t1.platform,
t1.main_channel,
t1.is_new
with cube
) t7 on t3.datasource=t7.datasource and t3.country=t7.region_code and t3.os_type=t7.platform and t3.main_channel=t7.main_channel and t3.is_new=t7.is_new
union all
select
/*+ REPARTITION(1) */
to_date('${cur_date}') as event_date,
'app_group' datasource,
t3.country,
t3.os_type,
t3.main_channel,
t3.is_new,
t3.dau,
t3.homepage_dau,
t3.pd_uv,
t3.pd_pv,
t3.pd_to_cart_uv,
t3.cart_uv,
t3.cart_to_checkout_uv,
t3.checkout_uv,
t3.pd_buy_uv,
t3.cart_success_uv,
t3.cart_success_pv,
t3.cart_buy_confirm_uv,
t3.checkout_place_order_uv,
t3.checkout_credit_card_uv,
nvl(t3.first_order_num,0) first_order_num,
nvl(t4.payed_user_num,0) payed_user_num,
nvl(t4.payed_order_num,0) payed_order_num,
nvl(t4.gmv,0) gmv,
nvl(t5.ordered_user_num,0) ordered_user_num,
nvl(t5.ordered_order_num,0) ordered_order_num,
nvl(t6.inac_payed_user_num,0) inac_payed_user_num,
nvl(t6.inac_payed_order_num,0) inac_payed_order_num,
nvl(t6.inac_gmv,0) inac_gmv,
nvl(t6.first_inac_payed_order_num,0) first_inac_payed_order_num,
nvl(t7.lck_gmv,0) lck_gmv,
'all' AS is_brand,
nvl(t4.brand_gmv,0) brand_gmv,
nvl(t4.no_brand_gmv,0) no_brand_gmv
from
(
select
nvl(t2.geo_country,'all') country,
nvl(t2.os_type,'all') os_type,
nvl(t2.main_channel,'all') main_channel,
nvl(t2.is_new,'all') is_new,
count(distinct t2.device_id) as dau,
count(distinct t2.homepage_device_id) as homepage_dau,
count(distinct t2.pd_device_id) as pd_uv,
count(t2.pd_device_id) as pd_pv,
count(distinct t2.pd_to_cart_device_id) as pd_to_cart_uv,
count(distinct t2.cart_device_id) as cart_uv,
count(distinct t2.cart_to_checkout_device_id) as cart_to_checkout_uv,
count(distinct t2.checkout_device_id) as checkout_uv,
count(distinct t2.pdbuy_device_id) as pd_buy_uv,
count(distinct t2.cart_success_device_id) as cart_success_uv,
count(t2.cart_success_device_id) as cart_success_pv,
count(distinct t2.cart_buy_confirm_device_id) as cart_buy_confirm_uv,
count(distinct t2.checkout_place_order_device_id) as checkout_place_order_uv,
count(distinct t2.checkout_credit_card_device_id) as checkout_credit_card_uv,
count(distinct t2.first_order_id) as first_order_num
from
(
select
nvl(t1.geo_country,'NA') geo_country,
nvl(t1.os_type,'NA') os_type,
nvl(dd.main_channel,'NA') main_channel,
CASE WHEN datediff(t1.pt,dd.activate_time)<=0 THEN 'new'
  WHEN datediff(t1.pt,dd.activate_time)>=1 and datediff(t1.pt,dd.activate_time)<6 THEN '2-7'
  WHEN datediff(t1.pt,dd.activate_time)>=7 and datediff(t1.pt,dd.activate_time)<29 THEN '8-30'
  else '30+' END is_new,
CASE WHEN t1.event_name ='screen_view' THEN t1.device_id end device_id,
CASE when datediff(t1.pt,dd.first_pay_time)=0 THEN dd.first_order_id END first_order_id,
CASE when t1.page_code='homepage' and t1.event_name ='screen_view' THEN t1.device_id end homepage_device_id,
CASE when t1.page_code='product_detail' and t1.view_type='show' and t1.event_name ='screen_view' THEN t1.device_id end pd_device_id,
CASE when t1.page_code='cart' and t1.referrer like '%product_detail%' and t1.view_type='show' and t1.event_name ='screen_view' THEN t1.device_id end pd_to_cart_device_id,
CASE when t1.page_code='cart' and t1.view_type='show' and t1.event_name ='screen_view' THEN t1.device_id end cart_device_id,
CASE when t1.page_code like '%checkout%' and t1.view_type='show' and t1.referrer like '%cart%' and t1.event_name ='screen_view' THEN t1.device_id end cart_to_checkout_device_id,
CASE when t1.page_code like '%checkout%' and t1.view_type='show' and t1.event_name ='screen_view' THEN t1.device_id  end checkout_device_id,
CASE when t1.element_name='pdAddToCartClick' and t1.event_name ='common_click' THEN t1.device_id end pdbuy_device_id,
CASE when t1.page_code='product_detail' and t1.element_name='pdAddToCartSuccess' and t1.event_name ='common_click' THEN t1.device_id end cart_success_device_id,
CASE when t1.element_name='button_cart_checkout' and t1.event_name ='order_process' THEN t1.device_id end cart_buy_confirm_device_id,
CASE when t1.element_name='button_checkout_placeOrder' and t1.event_name ='order_process' THEN t1.device_id end checkout_place_order_device_id,
CASE WHEN t1.element_name='noCreditCardPayClick' and t1.event_name ='common_click' and page_code='checkout_credit_card' THEN t1.device_id end checkout_credit_card_device_id
from
(
select pt,datasource,event_name,geo_country,os_type,page_code,device_id,referrer,view_type,NULL element_name,NULL list_name from dwd.dwd_vova_log_screen_view where pt='${cur_date}' and platform ='mob' and os_type is not null and os_type !='' and device_id is not null
union all
select pt,datasource,event_name,geo_country,os_type,page_code,device_id,referrer,NULL view_type,element_name,list_name from dwd.dwd_vova_log_common_click where pt='${cur_date}' and platform ='mob' and os_type is not null and os_type !='' and device_id is not null
union all
select pt,datasource,event_name,geo_country,os_type,page_code,device_id,referrer,NULL view_type,element_name,NULL list_name from dwd.dwd_vova_log_order_process where pt='${cur_date}' and platform ='mob' and os_type is not null and os_type !='' and device_id is not null
) t1
INNER JOIN ods_vova_vtsf.ods_vova_acg_app a on a.data_domain = t1.datasource
left join dim.dim_vova_devices dd on dd.device_id = t1.device_id and dd.datasource=t1.datasource
where t1.datasource not in ('vova','airyclub')
) t2
group by
   t2.geo_country,
   t2.os_type,
   t2.main_channel,
   t2.is_new
  with cube
) t3 left join
(
select
nvl(t1.region_code,'all') region_code,
nvl(t1.platform,'all') platform,
nvl(t1.main_channel,'all') main_channel,
nvl(t1.is_new,'all') is_new,
count(distinct t1.buyer_id) as payed_user_num,
count(distinct t1.order_id) as payed_order_num,
sum(t1.shop_price * t1.goods_number + t1.shipping_fee) as gmv,
sum(brand_gmv) brand_gmv,
sum(no_brand_gmv) no_brand_gmv
from
(
select
nvl(fp.region_code,'NA') region_code,
nvl(fp.platform,'NA') platform,
nvl(dd.main_channel,'NA') main_channel,
CASE WHEN datediff(fp.pay_time,dd.activate_time)<=0 THEN 'new'
  WHEN datediff(fp.pay_time,dd.activate_time)>=1 and datediff(fp.pay_time,dd.activate_time)<6 THEN '2-7'
  WHEN datediff(fp.pay_time,dd.activate_time)>=7 and datediff(fp.pay_time,dd.activate_time)<29 THEN '8-30'
  else '30+' END is_new,
fp.order_id,
fp.buyer_id,
fp.device_id,
fp.shop_price,
fp.shipping_fee,
fp.goods_number,
case when ddog.brand_id>0 then fp.shop_price * fp.goods_number + fp.shipping_fee else 0 end brand_gmv,
case when ddog.brand_id=0 then fp.shop_price * fp.goods_number + fp.shipping_fee else 0 end no_brand_gmv
from
dwd.dwd_vova_fact_pay fp
left join dim.dim_vova_devices dd on dd.device_id = fp.device_id and dd.datasource=fp.datasource
inner join dim.dim_vova_order_goods ddog on ddog.order_goods_id = fp.order_goods_id
where to_date(fp.pay_time)='${cur_date}' and fp.from_domain like '%api%'
and (ddog.order_tag not like '%luckystar_activity_id%' or ddog.order_tag is null) and  fp.datasource not in ('vova','airyclub')
) t1
group by
   t1.region_code,
   t1.platform,
   t1.main_channel,
   t1.is_new
  with cube
)t4 on  t3.country=t4.region_code and t3.os_type=t4.platform and t3.main_channel=t4.main_channel and t3.is_new=t4.is_new
left join
(
select
nvl(t1.region_code,'all') region_code,
nvl(t1.platform,'all') platform,
nvl(t1.main_channel,'all') main_channel,
nvl(t1.is_new,'all') is_new,
count(distinct t1.buyer_id) as ordered_user_num,
count(distinct t1.order_id) as ordered_order_num
from
(
select
nvl(ddog.region_code,'NA') region_code,
nvl(ddog.platform,'NA') platform,
nvl(dd.main_channel,'null') main_channel,
CASE WHEN datediff(ddog.order_time,dd.activate_time)<=0 THEN 'new'
  WHEN datediff(ddog.order_time,dd.activate_time)>=1 and datediff(ddog.order_time,dd.activate_time)<6 THEN '2-7'
  WHEN datediff(ddog.order_time,dd.activate_time)>=7 and datediff(ddog.order_time,dd.activate_time)<29 THEN '8-30'
  else '30+' END is_new,
ddog.order_id,
ddog.buyer_id,
ddog.device_id
from dim.dim_vova_order_goods ddog
left join dim.dim_vova_devices dd on dd.device_id = ddog.device_id and dd.datasource=ddog.datasource
where to_date(ddog.order_time) = '${cur_date}' and ddog.from_domain like '%api%' and  ddog.datasource not in ('vova','airyclub')
and (ddog.order_tag not like '%luckystar_activity_id%' or ddog.order_tag is null)
) t1
group by
   t1.region_code,
   t1.platform,
   t1.main_channel,
   t1.is_new
  with cube
) t5 on t3.country=t5.region_code and t3.os_type=t5.platform and t3.main_channel=t5.main_channel and t3.is_new=t5.is_new
left join
(
select
nvl(t1.region_code,'all') region_code,
nvl(t1.platform,'all') platform,
nvl(t1.main_channel,'all') main_channel,
nvl(t1.is_new,'all') is_new,
count(distinct t1.buyer_id) as inac_payed_user_num,
count(distinct t1.order_id) as inac_payed_order_num,
sum(t1.shop_price * t1.goods_number + t1.shipping_fee) as inac_gmv,
count(distinct t1.first_order_id) as first_inac_payed_order_num
from
(
select
nvl(fp.region_code,'NA') region_code,
nvl(fp.platform,'NA') platform,
nvl(dd.main_channel,'NA') main_channel,
CASE WHEN datediff(fp.pay_time,dd.activate_time)<=0 THEN 'new'
  WHEN datediff(fp.pay_time,dd.activate_time)>=1 and datediff(fp.pay_time,dd.activate_time)<6 THEN '2-7'
  WHEN datediff(fp.pay_time,dd.activate_time)>=7 and datediff(fp.pay_time,dd.activate_time)<29 THEN '8-30'
  else '30+' END is_new,
fp.order_id,
fp.buyer_id,
fp.device_id,
fp.shop_price,
fp.shipping_fee,
fp.goods_number,
CASE when datediff(fp.pay_time,dd.first_pay_time)=0 THEN dd.first_order_id END first_order_id
from
dwd.dwd_vova_fact_pay fp
left join dim.dim_vova_devices dd on dd.device_id = fp.device_id and dd.datasource=fp.datasource
inner join dim.dim_vova_order_goods ddog on ddog.order_goods_id = fp.order_goods_id
where to_date(fp.pay_time)='${cur_date}'and fp.from_domain like '%api%' and  ddog.datasource not in ('vova','airyclub')
and ddog.order_tag is null
) t1
group by
   t1.region_code,
   t1.platform,
   t1.main_channel,
   t1.is_new
  with cube
) t6 on t3.country=t6.region_code and t3.os_type=t6.platform and t3.main_channel=t6.main_channel and t3.is_new=t6.is_new
left join
(
select
nvl(t1.region_code,'all') region_code,
nvl(t1.platform,'all') platform,
nvl(t1.main_channel,'all') main_channel,
nvl(t1.is_new,'all') is_new,
sum(lck_gmv) as lck_gmv
from
(
select
nvl(fag.rgn_code,'NA') region_code,
nvl(fag.platform,'NA') platform,
nvl(dd.main_channel,'NA') main_channel,
CASE WHEN datediff(fag.pay_time,dd.activate_time)<=0 THEN 'new'
  WHEN datediff(fag.pay_time,dd.activate_time)>=1 and datediff(fag.pay_time,dd.activate_time)<6 THEN '2-7'
  WHEN datediff(fag.pay_time,dd.activate_time)>=7 and datediff(fag.pay_time,dd.activate_time)<29 THEN '8-30'
  else '30+' END is_new,
fag.ord_amt + fag.ship_fee as lck_gmv
from
dwd.dwd_vova_fact_act_ord_gs fag
left join dwd.dwd_vova_fact_buyer_device_releation fbr on fag.byr_id = fbr.buyer_id and fag.datasource = fbr.datasource
left join dim.dim_vova_devices dd on dd.device_id = fbr.device_id and dd.datasource=fbr.datasource
where to_date(fag.pay_time)='${cur_date}' and fbr.pt='${cur_date}' and fag.pay_sts>=1 and fag.datasource not in ('vova','airyclub')
) t1
group by
t1.region_code,
t1.platform,
t1.main_channel,
t1.is_new
with cube
) t7 on t3.country=t7.region_code and t3.os_type=t7.platform and t3.main_channel=t7.main_channel and t3.is_new=t7.is_new
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql --conf "spark.app.name=dwb_vova_main_process_zhangyin" --conf "spark.dynamicAllocation.maxExecutors=100" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
