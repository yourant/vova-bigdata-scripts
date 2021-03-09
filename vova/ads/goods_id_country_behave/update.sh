#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pt=$(date -d "-1 day" +%Y-%m-%d)
fi
pre_w=`date -d "6 day ago ${pt}" +%Y-%m-%d`
echo "$pre_w"

sql="
--商品分国家近七日表现
with ads_goods_id_country_behave_ctr as
(
select
goods_id,
geo_country country,
sum(clicks) clicks,
sum(impressions) impressions,
count(distinct clk_buyer_id) users,
count(distinct expre_buyer_id) impression_users,
nvl((sum(clicks)+0.015*50)/(sum(impressions)+50),0) ctr
from
(
select
g.goods_id,
geo_country,
buyer_id,
clicks,
impressions,
case when event_name='goods_click' then buyer_id else null end clk_buyer_id,
case when event_name='goods_impression' then buyer_id else null end expre_buyer_id
from
(
select event_name,buyer_id,geo_country,virtual_goods_id,1 clicks,0 impressions from dwd.dwd_vova_log_goods_click where pt>='$pre_w' and pt<='$pt' and dp ='vova' and buyer_id>0 and geo_country is not null and  geo_country !='' and platform ='mob'
union all
select event_name,buyer_id,geo_country,virtual_goods_id goods_id,0 clicks,1 impressions from dwd.dwd_vova_log_goods_impression where pt>='$pre_w' and pt<='$pt' and dp ='vova' and buyer_id>0 and geo_country is not null and  geo_country !='' and platform ='mob'
) t join dim.dim_vova_goods g on g.virtual_goods_id = t.virtual_goods_id
) t1 group by goods_id,geo_country
),

--商品近七日gmv
ads_goods_id_country_behave_gmv as
(
select
goods_id,
region_code country,
sum(shop_price * goods_number + shipping_fee)  gmv,
sum(goods_number)  sales_order,
count(distinct buyer_id) payed_user_num
from dwd.dwd_vova_fact_pay where to_date(pay_time)>='$pre_w' and region_code is not null and region_code !=''
group by goods_id,region_code
)
insert overwrite table ads.ads_vova_goods_id_country_behave partition(pt='${pt}')
select
/*+ REPARTITION(10) */
ctr.goods_id,
ctr.country,
clicks,
impressions,
nvl(sales_order,0) sales_order,
users,
impression_users,
nvl(gmv,0) gmv,
nvl(ctr,0) ctr,
case when impressions<2000 then 0.003 else nvl((gmv/users)*ctr,0) end gcr,
nvl(payed_user_num/impression_users,0) cr,
nvl(payed_user_num /users,0) click_cr
from ads_goods_id_country_behave_ctr ctr
left join ads_goods_id_country_behave_gmv gmv on ctr.goods_id = gmv.goods_id and ctr.country = gmv.country;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql  --conf "spark.app.name=ads_vova_goods_id_country_behave_zhangyin" --conf "spark.dynamicAllocation.maxExecutors=150" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi