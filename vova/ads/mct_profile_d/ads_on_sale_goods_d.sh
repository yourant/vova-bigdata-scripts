#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
insert overwrite table ads.ads_on_sale_goods_d PARTITION (pt = '${pre_date}')
select
g.goods_id,
g.merchant_id,
c.first_cat_id
from ods.vova_goods_arc g
inner join dwd.dim_category c on c.cat_id = g.cat_id
where pt ='$pre_date' and g.is_on_sale=1 and g.is_display=1 and g.is_delete=0;
"
spark-sql --conf "spark.app.name=ads_on_sale_goods_d" -e "$sql"
if [ $? -ne 0 ]; then
  exit 1
fi