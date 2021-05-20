#!/bin/bash
#指定日期和引擎
pre_w=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pre_w=`date -d "-7 day" +%Y-%m-%d`
fi

pt=$2
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
fi

sql="
drop table if exists tmp.ads_test_cat_gmv_1w;
create table tmp.ads_test_cat_gmv_1w as
select
/*+ REPARTITION(1) */
goods_id,
is_brand,
first_cat_id,
second_cat_id,
region_code geo_country,
gmv
from
(
select
goods_id,
is_brand,
first_cat_id,
second_cat_id,
region_code,
gmv,
row_number() OVER (PARTITION BY region_code,first_cat_id,second_cat_id,is_brand ORDER BY gmv DESC) AS rank
from
(
select
og.goods_id,
g.first_cat_id,
g.second_cat_id,
if(g.brand_id>0,1,0) is_brand,
og.region_code,
sum(og.goods_number * og.shop_price + og.shipping_fee) as gmv
from dwd.fact_pay og
left join dwd.dim_goods g on og.goods_id = g.goods_id
where date(og.pay_time) >='$pre_w'
and og.datasource ='vova'
and og.region_code in ('GB','FR','DE','IT','ES')
and og.platform in ('ios','android')
and g.is_on_sale =1
group by og.goods_id,g.first_cat_id,g.second_cat_id,og.region_code,if(g.brand_id>0,1,0)
) t
) t1 where rank <=30;

drop table if exists tmp.ads_test_cat_ctr_1w;
create table tmp.ads_test_cat_ctr_1w as
select
/*+ REPARTITION(1) */
g.goods_id,
g.first_cat_id,
g.second_cat_id,
if(g.brand_id>0,1,0) is_brand,
t.geo_country,
sum(clicks) clicks,
sum(impressions) impressions,
sum(clicks)/sum(impressions) ctr,
count(distinct click_device_id) click_uv
from
(
select geo_country,virtual_goods_id, device_id click_device_id,null impression_device_id,1 clicks,0 impressions from dwd.fact_log_goods_click where pt>='$pre_w' and platform='mob' and datasource='vova' and geo_country in ('GB','FR','DE','IT','ES')
union all
select geo_country,virtual_goods_id,null click_device_id,device_id impression_device_id,0 clicks,1 impressions from dwd.fact_log_goods_impression where pt>='$pre_w' and platform='mob' and datasource='vova' and geo_country in ('GB','FR','DE','IT','ES')
) t
join dwd.dim_goods g on g.virtual_goods_id = t.virtual_goods_id
join (select distinct goods_id from tmp.ads_test_cat_gmv_1w) t1 on t1.goods_id =g.goods_id
where g.is_on_sale = 1
group by g.goods_id,g.first_cat_id,g.second_cat_id,t.geo_country,if(g.brand_id>0,1,0);


drop table if exists tmp.ads_test_cat_gcr_1w;
create table tmp.ads_test_cat_gcr_1w as
select
/*+ REPARTITION(1) */
t1.goods_id,
t1.is_brand,
t1.first_cat_id,
t1.second_cat_id,
t1.geo_country,
t2.clicks,
t2.impressions,
t2.ctr,
t2.click_uv,
nvl(t1.gmv,0) gmv,
nvl(t1.gmv/nvl(t2.click_uv,0) *nvl(t2.ctr,0) * 10000,0) gcr
from tmp.ads_test_cat_gmv_1w t1
left join tmp.ads_test_cat_ctr_1w t2 on t1.goods_id = t2.goods_id and t1.geo_country = t2.geo_country
where t2.impressions>=500;

drop table if exists tmp.ads_test_cat_gcr_res_1w;
create table tmp.ads_test_cat_gcr_res_1w as
select
first_cat_id,
nvl(second_cat_id,0) second_cat_id,
is_brand,
geo_country,
case when is_brand =0 and gcr <60 then 60
     when is_brand = 0 and gcr>=60 then gcr
     when is_brand =1 and gcr <120 then 120
     when is_brand =1 and gcr >= 120 then gcr
     end gcr
from
(
select
goods_id,
is_brand,
first_cat_id,
second_cat_id,
geo_country,
gcr
from
(
select
goods_id,
is_brand,
first_cat_id,
second_cat_id,
geo_country,
clicks,
impressions,
ctr,
click_uv,
gmv,
gcr,
row_number() OVER (PARTITION BY geo_country,first_cat_id,second_cat_id,is_brand ORDER BY gcr) AS rank
from tmp.ads_test_cat_gcr_1w
) t where rank =1
) t1;
"
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" --conf "spark.sql.adaptive.shuffle.targetPostShuffleInputSize=128000000" --conf "spark.sql.adaptive.enabled=true" --conf "spark.app.name=test_cat_gcr_1w" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
--connect jdbc:mysql://rec-backend.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com/backend \
--username dwbackendwriter --password Rap11rJQZE3ATA18GZHAbySsNZVIvjnE \
--table ads_test_cat_gcr_1w \
--update-key "first_cat_id,is_brand,second_cat_id,geo_country" \
--update-mode allowinsert \
--hcatalog-database tmp \
--hcatalog-table ads_test_cat_gcr_res_1w \
--fields-terminated-by '\001'

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
