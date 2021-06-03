#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1

#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date: ${cur_date}"

table_suffix=`date -d "${cur_date}" +%Y%m%d`
echo "table_suffix: ${table_suffix}"

job_name="mlb_vova_goods_second_cat_req8721_chenkai_${cur_date}"

###逻辑sql
sql="
ALTER TABLE mlb.mlb_vova_goods_second_cat DROP if exists partition(pt = '$(date -d "${cur_date:0:10} -5day" +%Y-%m-%d)');

insert overwrite table mlb.mlb_vova_goods_second_cat partition(pt='${cur_date}')
select /*+ REPARTITION(5) */
  virtual_goods_id,
  goods_id,
  nvl(second_cat_id, 0) second_cat_id,
  nvl(cat_id, 0) cat_id,
  group_id,
  nvl(brand_id, -1) brand_id
from
  dim.dim_vova_goods
where is_on_sale = 1
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
