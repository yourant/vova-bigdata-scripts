#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
insert overwrite table ads.ads_vova_img_enhance_d PARTITION (pt = '${pre_date}')
select
/*+ REPARTITION(1) */
img_id,
goods_id,
img_url,
is_default
from
(
select
gg.img_id,
gg.goods_id,
gg.img_url,
gg.is_default
from ods_vova_vteos.ods_vova_goods_gallery gg
join (select
goods_id
from dwd.dwd_vova_fact_pay p
join ads.ads_vova_mct_rank r on p.mct_id = r.mct_id and p.first_cat_id = r.first_cat_id
where r.pt='2021-03-28' and r.rank<=3
group by p.goods_id) t on gg.goods_id = t.goods_id
union all
select
gg.img_id,
gg.goods_id,
gg.img_url,
gg.is_default
from ods_vova_vteos.ods_vova_goods_gallery gg
join dim.dim_vova_goods g on gg.goods_id = g.goods_id
join ads.ads_vova_mct_rank r on g.mct_id = r.mct_id and g.first_cat_id = r.first_cat_id
where r.pt='2021-03-28' and r.rank>3 and g.is_on_sale =1
) t ;
"

spark-sql  --conf "spark.app.name=ads_vova_img_enhance_d_zhangyin" --conf "spark.dynamicAllocation.maxExecutors=100"  -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi