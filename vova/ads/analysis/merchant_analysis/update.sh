#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
insert overwrite table ads.ads_vova_merchant_analysis partition(pt='${pre_date}')
select
ga.datasource,
ga.device_id,
ga.goods_id,
ga.page_code,
ga.list_type,
ga.clk_cnt,
ga.expre_cnt,
ga.sales_vol,
ga.gmv,
ga.is_brand,
ga.first_cat_name,
ga.first_cat_id,
ga.second_cat_name,
ga.second_cat_id,
ga.mct_id,
ga.mct_name,
mr.rank as mct_rank
from
ads.ads_vova_goods_analysis ga
left join ads.ads_vova_mct_rank mr
on ga.first_cat_id=mr.first_cat_id and ga.mct_id=mr.mct_id and mr.pt='${pre_date}'
where ga.pt='${pre_date}';
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=ads_vova_merchant_analysis" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.autoBroadcastJoinThreshold=-1" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
