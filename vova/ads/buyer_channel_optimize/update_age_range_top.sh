#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
#离线更新一次
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "cur_date: $cur_date"

job_name="ads_vova_age_range_top_req8523_chenkai_${cur_date}"

#
sql="
insert overwrite table ads.ads_vova_age_range_top partition(pt='${cur_date}')
select
  goods_id,
  age_range
from
(
  select
    goods_id,
    age_range,
    goods_number,
    row_number() over(partition by goods_id, age_range order by goods_number desc) row
  from
  (
    select
      goods_id,
      age_range,
      sum(goods_number) goods_number
    from
    (
      select
        fp.goods_id,
        fp.goods_number,
        fp.order_goods_id,
        case when year(current_date) - year(db.birthday) > 0 and year(current_date) - year(db.birthday) <= 18 then '0-18'
          when year(current_date) - year(db.birthday) > 18 and year(current_date) - year(db.birthday) <= 35 then '18-35'
          else 'other'
        end age_range
      from
        dwd.dwd_vova_fact_pay fp
      left join
        dim.dim_vova_goods dg
      on fp.datasource = dg.datasource and fp.goods_id = dg.goods_id
      left join
        dim.dim_vova_buyers db
      on fp.datasource = db.datasource and fp.buyer_id = db.buyer_id
      where fp.datasource = 'vova'
        and date(birthday)>'1970-01-01' and date(birthday) != '1990-01-01'
        and year(db.birthday) is not null
        and dg.is_on_sale = 1
    )
    group by age_range, goods_id
  )
  where age_range in ('0-18', '18-35')
)
where row <= 100
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
