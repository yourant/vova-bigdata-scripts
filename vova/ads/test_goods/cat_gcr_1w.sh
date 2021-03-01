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
drop table if exists tmp.test_cat_ctr_1w;
create table tmp.test_cat_ctr_1w as
select
/*+ REPARTITION(1) */
g.goods_id,
t.geo_country,
sum(clicks) clicks,
sum(impressions) impressions,
sum(clicks)/sum(impressions) ctr,
count(distinct click_device_id) click_uv
from
(
select geo_country,virtual_goods_id, device_id click_device_id,null impression_device_id,1 clicks,0 impressions from dwd.fact_log_goods_click where pt>='$pre_w' and platform='mob' and datasource='vova' and page_code in ('homepage','product_list') and  list_type in ('/product_list_popular','product_list_popular') and geo_country in ('GB','FR','DE','IT','ES')
union all
select geo_country,virtual_goods_id,null click_device_id,device_id impression_device_id,0 clicks,1 impressions from dwd.fact_log_goods_impression where pt>='$pre_w' and platform='mob' and datasource='vova' and page_code in ('homepage','product_list') and  list_type in ('/product_list_popular','product_list_popular') and geo_country in ('GB','FR','DE','IT','ES')
) t
join dwd.dim_goods g on g.virtual_goods_id = t.virtual_goods_id
where g.brand_id = 0
group by g.goods_id,geo_country;

drop table if exists tmp.test_cat_gmv_1w;
create table tmp.test_cat_gmv_1w as
select
/*+ REPARTITION(1) */
g.second_cat_id,
og.region_code,
sum(og.goods_number * og.shop_price + og.shipping_fee) as gmv
from dwd.fact_pay og
join dwd.fact_order_cause_v2 oc on og.order_goods_id = oc.order_goods_id
join dwd.dim_goods g on g.goods_id = og.goods_id
where date(og.pay_time) >='$pre_w' and oc.pt>='$pre_w'
and oc.datasource ='vova'
and oc.pre_page_code in ('homepage','product_list') and oc.pre_list_type in ('/product_list_popular','product_list_popular')
and og.region_code in ('GB','FR','DE','IT','ES') and g.brand_id = 0
group by g.second_cat_id,region_code;

drop table if exists tmp.test_cat_gcr_1w;
create table tmp.test_cat_gcr_1w as
select
/*+ REPARTITION(1) */
t1.geo_country,
t1.second_cat_id,
t1.clicks,
t1.impressions,
t1.ctr,
t1.click_uv,
nvl(gmv,0) gmv,
nvl(nvl(t2.gmv,0)/t1.click_uv *t1.ctr * 10000,0) gcr
from tmp.test_cat_ctr_1w t1
left join tmp.test_cat_gmv_1w t2 on t1.goods_id = t2.goods_id and t1.geo_country = t2.region_code and t1.second_cat_id = t2.second_cat_id
;

drop table if exists tmp.test_cat_gcr_res_1w;
create table tmp.test_cat_gcr_res_1w as
select
second_cat_id,
geo_country,
case when avg(gcr) =0 then 10 else  avg(gcr) end gcr
from
tmp.test_cat_gcr_1w where second_cat_id is not null and geo_country is not null
group by second_cat_id,geo_country;

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
--username bimaster --password kkooxGjFy7Vgu21x \
--table test_cat_gcr_1w \
--update-key "second_cat_id,geo_country" \
--update-mode allowinsert \
--hcatalog-database tmp \
--hcatalog-table test_cat_gcr_res_1w \
--fields-terminated-by '\001'

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
