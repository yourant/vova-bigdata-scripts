#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
insert overwrite table ads.ads_vova_mct_d PARTITION (pt = '$pre_date')
select
/*+ REPARTITION(1) */
mct_id merchant_id,
if(tag='tag1' or tag='tag2',1,0) act_mct_2m
from dim.dim_vova_merchant;
"
spark-sql  --conf "spark.app.name=ads_vova_mct_d_zhangyin" --conf "spark.dynamicAllocation.maxExecutors=100" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi