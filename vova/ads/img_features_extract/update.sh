#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
his_date=`date -d "365 days ago ${pre_date}" +%Y-%m-%d`

sql="
insert overwrite table ads.ads_vova_img_features_extract_d PARTITION (pt = '${pre_date}')
select
/*+ REPARTITION(1) */
gg.img_id,
gg.goods_id,
gg.img_url
from dwd.dwd_vova_fact_pay p
join ods_vova_vteos.ods_vova_goods_gallery gg on p.goods_id = gg.goods_id
join dim.dim_vova_goods g on g.goods_id = p.goods_id
where g.is_on_sale =1  and gg.is_delete=0 and g.brand_id=0 and to_date(pay_time)>='$his_date';
"
spark-sql  --conf "spark.app.name=ads_vova_img_features_extract_d_zhangyin" --conf "spark.dynamicAllocation.maxExecutors=100"  -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
