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

table_suffix=`date -d "${cur_date}" +%Y%m%d`
echo "table_suffix: ${table_suffix}"

job_name="ads_vova_channel_region_top_req8523_chenkai_${cur_date}"

#
sql="
create table if not EXISTS tmp.tmp_ads_channel_region_top_${table_suffix} as
select
  region_code,
  buyer_cnt,
  row
from
(
  select
    region_code,
    buyer_cnt,
    row_number() over(order by buyer_cnt desc) row
  from
  (
    select
      dd.region_code region_code,
      count(distinct db.buyer_id) buyer_cnt
    from
      dim.dim_vova_buyers db
    left join
      dim.dim_vova_devices dd
    on db.current_device_id = dd.device_id
    where dd.child_channel = 'bytedanceglobal_int' and dd.device_id is not null
    group by dd.region_code
  )
) where row <= 5
;

insert overwrite table ads.ads_vova_channel_region_top partition(pt='${cur_date}')
select
  goods_id,
  region_id,
  region_code,
  goods_number,
  'bytedanceglobal_int' channel
from
(
  select
    t1.goods_id,
    nvl(dr.region_id, -1) region_id,
    t1.region_code,
    t1.goods_number,
    row_number() over(partition by t1.region_code order by goods_number desc) row
  from
    (
      select
        fp.region_code,
        fp.goods_id,
        fp.goods_number
      from
      (
        select
          nvl(region_code, '-1') region_code,
          goods_id,
          sum(goods_number) goods_number
        from
          dwd.dwd_vova_fact_pay
        where datasource = 'vova'
        group by cube(region_code, goods_id)
      ) fp
      left join
        dim.dim_vova_goods dg
      on fp.goods_id = dg.goods_id
      where dg.datasource = 'vova'
        and dg.is_on_sale = 1
        and fp.goods_id is not null
        and region_code in (
          select
            '-1' as region_code
          union all
          select
            region_code
          from
            tmp.tmp_ads_channel_region_top_${table_suffix}
        )
    ) t1
    left join
    (
      select distinct
        region_id region_id,country_code region_code
      from dim.dim_vova_region where parent_id = 0
    ) dr
    on t1.region_code = dr.region_code
    where t1.goods_id is not null
) where row <= 200
;

drop table if EXISTS tmp.tmp_ads_channel_region_top_${table_suffix};
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
