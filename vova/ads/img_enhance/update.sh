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
gg.img_id,
gg.goods_id,
gg.img_url
from ods_vova_vteos.ods_vova_goods_gallery gg
join (select
goods_id
from dim.dim_vova_goods where is_on_sale =1 order by goods_id desc limit 1000) g on gg.goods_id = g.goods_id;
"

spark-sql  --conf "spark.app.name=ads_vova_img_enhance_d_zhangyin" --conf "spark.dynamicAllocation.maxExecutors=100"  -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi