#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
#194,Women's Clothing
#5768,Men's Clothing
#5777,Shoes
#5883,Athletic Shoes
#5715,"Bags, Watches & Accessories"
#5741,Bags & Packs
#5943,Belts
#5902,Necklaces & Pendants
#5905,Rings
#5954,Wallets
#5909,Hair Jewelry
#5712,Home & Garden
#5769,Health & Beauty

#delete 2021.01.20 vvjoys, dearbuys, vvshein, shejoys
#vvjoys
## and dg.first_cat_id in (5715) and dg.second_cat_id in (5741, 5943, 5902, 5905, 5954, 5909)
#dearbuys
## and dg.first_cat_id = 5712
## modify now and dg.first_cat_id in (194, 5768, 5777)
#vvshein
## and dg.first_cat_id = 194
#shejoys
## and dg.first_cat_id = 5769
#florynight
##AND (
##dg.first_cat_id in (194, 5768)
##OR
##(dg.first_cat_id in (5777) and dg.second_cat_id != 5883)
##OR
##(dg.first_cat_id in (5715) and dg.second_cat_id in (5741, 5943, 5902, 5905, 5954, 5909) )
##)

sql="
INSERT OVERWRITE TABLE ads.ads_vova_site_goods_from_vova PARTITION (pt = '${cur_date}')
select
new_data.event_date,
project.project_name as datasource,
new_data.goods_id,
new_data.virtual_goods_id,
new_data.impressions,
new_data.gcr,
new_data.goods_type
from
(
select
       '${cur_date}' AS event_date,
       t1.goods_id,
       first(dg.virtual_goods_id) as virtual_goods_id,
       first(t1.impressions) as impressions,
       first(t1.gcr) as gcr,
       first(t1.goods_type) as goods_type
from
(
SELECT
       if(min_goods.goods_id IS NULL, agp.goods_id , min_goods.min_price_goods_id) AS goods_id,
       if(min_goods.goods_id IS NULL, agp.goods_id , agp.goods_id) AS goods_type,
       agp.impressions,
       agp.gcr
FROM ads.ads_vova_goods_performance agp
inner join dim.dim_vova_goods dg on dg.goods_id = agp.goods_id
LEFT JOIN (SELECT min_goods.min_price_goods_id, min_goods.goods_id FROM ads.ads_vova_min_price_goods_h min_goods WHERE min_goods.pt = '${cur_date}' AND min_goods.strategy = 'c') min_goods ON min_goods.goods_id = dg.goods_id
WHERE agp.pt = '${cur_date}'
AND ((agp.gcr > 80 AND agp.impressions > 5000) or agp.gmv > 500)
AND agp.datasource = 'vova'
AND agp.platform = 'all'
AND agp.region_code = 'all'
AND (
dg.first_cat_id in (194, 5768)
OR
(dg.first_cat_id in (5777) and dg.second_cat_id != 5883)
OR
(dg.first_cat_id in (5715) and dg.second_cat_id in (5741, 5943, 5902, 5905, 5954, 5909) )
)
) t1
inner join dim.dim_vova_goods dg on dg.goods_id = t1.goods_id
AND dg.brand_id = 0
AND dg.is_on_sale = 1
group by t1.goods_id
) new_data
left join (select distinct goods_id from ads.ads_vova_site_goods_from_vova where pt < '${cur_date}' and datasource = 'florynight' ) old_data on new_data.goods_id = old_data.goods_id
CROSS JOIN
(
    select project_name
from (select 'florynight,myclothbox,myclothbag,floryclub,sheinclub' AS project_name_list) t
lateral view explode(split(project_name_list,',')) num as project_name
) project on 1=1
where old_data.goods_id is null

union all

select
new_data.event_date,
'airybox' as datasource,
new_data.goods_id,
new_data.virtual_goods_id,
new_data.impressions,
new_data.gcr,
new_data.goods_type
from
(
select
       '${cur_date}' AS event_date,
       t1.goods_id,
       first(dg.virtual_goods_id) as virtual_goods_id,
       first(t1.impressions) as impressions,
       first(t1.gcr) as gcr,
       first(t1.goods_type) as goods_type
from
(
SELECT
       if(min_goods.goods_id IS NULL, agp.goods_id , min_goods.min_price_goods_id) AS goods_id,
       if(min_goods.goods_id IS NULL, agp.goods_id , agp.goods_id) AS goods_type,
       agp.impressions,
       agp.gcr
FROM ads.ads_vova_goods_performance agp
inner join dim.dim_vova_goods dg on dg.goods_id = agp.goods_id
LEFT JOIN (SELECT min_goods.min_price_goods_id, min_goods.goods_id FROM ads.ads_vova_min_price_goods_h min_goods WHERE min_goods.pt = '${cur_date}' AND min_goods.strategy = 'c') min_goods ON min_goods.goods_id = dg.goods_id
WHERE agp.pt = '${cur_date}'
AND agp.gcr > 80
AND agp.impressions > 5000
AND agp.datasource = 'vova'
AND agp.platform = 'all'
AND agp.region_code = 'all'
AND dg.first_cat_id in (5768)

UNION

(
SELECT
       if(min_goods.goods_id IS NULL, agp.goods_id , min_goods.min_price_goods_id) AS goods_id,
       if(min_goods.goods_id IS NULL, agp.goods_id , agp.goods_id) AS goods_type,
       agp.impressions,
       agp.gcr
FROM ads.ads_vova_goods_performance agp
inner join dim.dim_vova_goods dg on dg.goods_id = agp.goods_id
LEFT JOIN (SELECT min_goods.min_price_goods_id, min_goods.goods_id FROM ads.ads_vova_min_price_goods_h min_goods WHERE min_goods.pt = '${cur_date}' AND min_goods.strategy = 'c') min_goods ON min_goods.goods_id = dg.goods_id
WHERE agp.pt = '${cur_date}'
AND agp.datasource = 'vova'
AND agp.platform = 'all'
AND agp.region_code = 'all'
AND dg.first_cat_id in (5768)
AND dg.brand_id = 0
ORDER BY agp.gmv DESC
limit 100
)

) t1
inner join dim.dim_vova_goods dg on dg.goods_id = t1.goods_id
AND dg.brand_id = 0
AND dg.is_on_sale = 1
group by t1.goods_id
) new_data
left join (select distinct goods_id from ads.ads_vova_site_goods_from_vova where pt < '${cur_date}' and datasource = 'airybox' ) old_data on new_data.goods_id = old_data.goods_id
where old_data.goods_id is null

;

"

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=20" --conf "spark.app.name=site_vova_goods" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

