#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
insert overwrite table ads.ads_vova_goods_d
select
/*+ REPARTITION(4) */
goods_id,
virtual_goods_id,
nvl(shop_price,0) shop_price,
nvl(shipping_fee,0) shipping_fee,
cat_id,
brand_id
from
dim.dim_vova_goods
"

spark-sql --conf "spark.app.name=ads_vova_goods_d_zhangyin"  --conf "spark.dynamicAllocation.maxExecutors=100" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi