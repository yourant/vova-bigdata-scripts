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

job_name="ads_vova_activity_mothers_day_req9345_chenkai_${cur_date}"

###逻辑sql 只跑一版
sql="
insert overwrite table ads.ads_vova_activity_mothers_day partition(pt = '2021-04-25')
select /*+ REPARTITION(1) */
  tg.goods_id,
  0 region_id,
  case when agp.first_cat_id = 194 then 'mqj_women'
    when agp.first_cat_id = 5715 then 'mqj_bags'
    when agp.first_cat_id = 5769 then 'mqj_health'
    when agp.first_cat_id = 5777 then 'mqj_shoes'
    when agp.first_cat_id = 5712 then 'mqj_home'
    when agp.first_cat_id = 5770 then 'mqj_baby'
    else 'mqj_others'
  end biz_type,
  3 rp_type,
  agp.first_cat_id first_cat_id,
  nvl(agp.second_cat_id, 0) second_cat_id,
  row_number() over(ORDER BY agp.cr_rate_15d desc) rank
from
(
  select
    distinct goods_id
  from
    tmp.tmp_activity_mothers_day_goods
) tg
left join
  ads.ads_vova_goods_portrait agp
on tg.goods_id = agp.goods_id
where agp.pt = '2021-04-25'
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
--conf "spark.dynamicAllocation.maxExecutors=120" \
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
