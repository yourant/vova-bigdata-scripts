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

job_name="dwb_vova_mct_first_cat_rank_downgrading_req_chenkai_${cur_date}"

###逻辑sql
sql="
insert overwrite table dwb.dwb_vova_mct_first_cat_rank_downgrading PARTITION (pt='${cur_date}')
select
/*+ REPARTITION(1) */
  'vova' datasource,
  regexp_replace(dm.spsor_name,'\'',' ') spsor_name,
  today.mct_id mct_id,
  regexp_replace(dm.mct_name,'\'',' ') mct_name,
  terday.rank terday_rank,
  today.rank today_rank,
  today.first_cat_id first_cat_id,
  regexp_replace(dg.first_cat_name,'\'',' ') first_cat_name
from
(
  select
    mct_id,
    first_cat_id,
    rank
  from
    ads.ads_vova_mct_rank
  where pt = '${cur_date}' and mct_id not in (26414, 11630, 36655)
    and rank in (5, 4, 3)
) today
left join
(
  select
    mct_id,
    first_cat_id,
    rank
  from
    ads.ads_vova_mct_rank
  where pt = date_sub('${cur_date}', 1) and mct_id not in (26414, 11630, 36655)
) terday
on today.mct_id = terday.mct_id and today.first_cat_id = terday.first_cat_id
  and today.rank < terday.rank
left join
  dim.dim_vova_merchant dm
on today.mct_id = dm.mct_id
left join
(
  select
    distinct first_cat_id, first_cat_name
  from
    dim.dim_vova_goods
) dg
on today.first_cat_id = dg.first_cat_id
where terday.rank is not null
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
