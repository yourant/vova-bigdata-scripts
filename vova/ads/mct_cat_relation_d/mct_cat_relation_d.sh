#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
s3path=`date -d "${pre_date} 00:00:00" +%Y/%m/%d`
###逻辑sql
sql="
insert overwrite table ads.ads_vova_mct_cat_relation PARTITION (pt = '${pre_date}')
select
/*+ REPARTITION(1) */
mct_id,
first_cat_id,
goods_id,
virtual_goods_id,
cat_id,
second_cat_id
from dim.dim_vova_goods
where is_on_sale =1 and first_cat_id is not null and second_cat_id is not null
"
spark-sql --conf "spark.app.name=ads_vova_mct_cat_relation_zhangyin" --conf "spark.dynamicAllocation.maxExecutors=100"  -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
