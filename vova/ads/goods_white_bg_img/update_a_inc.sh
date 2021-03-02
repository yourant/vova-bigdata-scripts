#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "cur_date: $cur_date"

job_name="ads_vova_goods_white_bg_img_a_inc_req8367_chenkai_${cur_date}"

#
sql="
-- a 每天增量: 从每天全量中过滤掉之前已经处理过的
insert overwrite table ads.ads_vova_goods_white_bg_img_a_inc partition(pt='${cur_date}')
select /*+ REPARTITION(1) */
  a.goods_id,
  a.sku_id,
  a.img_id,
  a.img_url
from
  ads.ads_vova_goods_white_bg_img_a_arc a
left join
(
  select distinct
    goods_id goods_id,
    img_id img_id
  from
    ads.ads_vova_goods_white_bg_img_a_inc
  where pt < '${cur_date}'
) a_res
on a.goods_id = a_res.goods_id and a.img_id = a_res.img_id
where a.pt='${cur_date}' and a_res.img_id is null
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
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
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
