#!/bin/bash
#指定日期和引擎
cur_date=$1
pre_hour=$2
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=$(date "+%Y-%m-%d")
fi
if [ ! -n "$2" ];then
pre_hour=$(date "+%H")
fi

echo "time:${cur_date} ${pre_hour}"

sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ads.ads_goods_expre_analysis_h partition(pt,hour)
select
dg.goods_id,
dg.first_cat_name,
if(dg.brand_id>0,1,0) is_brand,
t1.page_code,
t1.expre_cnt,
mr.rank as mct_rank,
'${cur_date}' as pt,
'${pre_hour}' as hour
from
(select
imp.element_id as vir_goods_id,
imp.page_code,
count(*) expre_cnt
from
dwd.dwd_vova_log_impressions_arc imp
where imp.pt='${cur_date}' and imp.hour <= '${pre_hour}' and imp.event_type='goods'
group by imp.page_code,imp.element_id) t1
inner join dim.dim_vova_goods dg on t1.vir_goods_id = dg.virtual_goods_id
left join ads.ads_vova_mct_rank mr on  mr.pt = date_add('${cur_date}',-2) and dg.mct_id = mr.mct_id and dg.first_cat_id = mr.first_cat_id
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_goods_expre_analysis_h" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi
