#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date=$cur_date"
###先删除昨日分区，支持重跑
hive -e "ALTER TABLE ads.ads_vova_img_search_source_his DROP if exists partition(pt = '$cur_date');"
if [ $? -ne 0 ];then
  exit 1
fi
max_pt=$2
his_date=${max_pt}
if [ ! -n "$2" ];then
max_pt=$(hive -e "show partitions ads.ads_vova_img_search_source_his" | tail -1)
if [ $? -ne 0 ];then
  exit 1
fi
his_date=${max_pt:3}
fi
echo "his_date=$his_date"

sql="
with tmp_ads_vova_img_search_source_d as
(
select
img_id,
sku_id,
goods_id,
img_url,
cat_id,
first_cat_id,
first_cat_name,
second_cat_id,
second_cat_name,
is_default,
img_color,
brand_id
from
(
select
img_id,
sku_id,
goods_id,
img_url,
cat_id,
first_cat_id,
first_cat_name,
second_cat_id,
second_cat_name,
is_default,
row_number() OVER (PARTITION BY img_id ORDER BY is_default) AS rank,
img_color,
brand_id
from
(
select
img_id,
sku_id,
goods_id,
img_url,
cat_id,
first_cat_id,
first_cat_name,
second_cat_id,
second_cat_name,
is_default,
img_color,
brand_id
from
(
select
sku_id,
img_id,
goods_id,
img_url,
first_cat_id,
first_cat_name,
second_cat_id,
second_cat_name,
is_default,
img_color,
brand_id,
cat_id,
row_number() OVER (PARTITION BY img_color,goods_id ORDER BY is_default) AS rank
from
(
select
gs.sku_id,
gs.img_id,
gs.goods_id,
gg.img_url,
g.first_cat_id,
replace(g.first_cat_name, ',', '') first_cat_name,
g.second_cat_id,
replace(g.second_cat_name, ',', '') second_cat_name,
gg.is_default,
replace(gs.img_color, ',', '') img_color,
g.brand_id,
g.cat_id
from dim.dim_vova_goods_sku gs
join dim.dim_vova_goods g on gs.goods_id = g.goods_id
join ods_vova_vteos.ods_vova_goods_gallery gg on gs.img_id = gg.img_id
where g.is_on_sale = 1 and gs.is_delete =0 and gs.color_is_show =1 and  g.first_cat_id in (194,5768)
) t
) t where rank = 1
union all
select
gg.img_id,
-1 sku_id,
gg.goods_id,
gg.img_url,
g.cat_id,
g.first_cat_id,
replace(g.first_cat_name, ',', '') first_cat_name,
g.second_cat_id,
replace(g.second_cat_name, ',', '') second_cat_name,
gg.is_default,
null img_color,
g.brand_id
from ods_vova_vteos.ods_vova_goods_gallery  gg
join dim.dim_vova_goods g on gg.goods_id = g.goods_id
where g.is_on_sale = 1 and gg.is_default = 1
) t
) t where rank =1
)

INSERT OVERWRITE TABLE ads.ads_vova_img_search_source_d PARTITION (pt = '$cur_date')
select
/*+ REPARTITION(1) */
d.img_id,
d.sku_id,
d.goods_id,
d.img_url,
d.cat_id,
d.first_cat_id,
d.first_cat_name,
d.second_cat_id,
d.second_cat_name,
d.is_default,
d.img_color,
d.brand_id
from  tmp_ads_vova_img_search_source_d d
left join ads.ads_vova_img_search_source_his his on his.img_id = d.img_id and his.pt='$his_date'
where his.img_id is null;

INSERT OVERWRITE TABLE ads.ads_vova_img_search_source_his PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(10) */
img_id,
sku_id,
goods_id,
img_url,
cat_id,
first_cat_id,
first_cat_name,
second_cat_id,
second_cat_name,
is_default,
img_color,
brand_id
from ads.ads_vova_img_search_source_his where pt='$his_date'
union all
select
/*+ REPARTITION(1) */
img_id,
sku_id,
goods_id,
img_url,
cat_id,
first_cat_id,
first_cat_name,
second_cat_id,
second_cat_name,
is_default,
img_color,
brand_id
from ads.ads_vova_img_search_source_d where pt='$cur_date';
"
spark-sql --conf "spark.app.name=ads_vova_img_search_source_d" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
