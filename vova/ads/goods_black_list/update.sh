#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
with tmp_goods as
(
select
goods_id
from
(
select
g.goods_id
from dim.dim_vova_goods g
join dim.dim_vova_merchant m on g.mct_id = m.mct_id
where m.spsor_name ='mogu'
union
select
goods_id
from dim.dim_vova_goods where is_on_sale =0
union
select
goods_id
from ads.ads_vova_goods_restrict_d
)
)
insert overwrite table ads.ads_vova_goods_black_list_arc PARTITION (pt = '${pre_date}')
select
/*+ REPARTITION(1) */
goods_id from tmp_goods;
insert overwrite table ads.ads_vova_goods_black_list
select
/*+ REPARTITION(1) */
goods_id from ads.ads_vova_goods_black_list_arc where pt='$pre_date';
"
spark-sql --conf "spark.app.name=ads_vova_goods_black_list_zhangyin"  --conf "spark.dynamicAllocation.maxExecutors=100" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
