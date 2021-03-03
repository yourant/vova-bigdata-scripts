#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1

#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date: ${cur_date}"

job_name="ads_vova_activity_home_garden_req7369_chenkai"

###逻辑sql
sql="
with tmp_home_garden as (
 select
   t1.goods_id goods_id,
   t1.region_id region_id,
   t1.expre_cnt expre_cnt,
   t1.clk_cnt clk_cnt,
   t1.ord_cnt ord_cnt,
   t1.gmv gmv,
   t1.sales_vol sales_vol,
   t1.users users,
   t1.click_uv click_uv,
   dg.first_cat_name first_cat_name,
   dg.first_cat_id first_cat_id,
   dg.second_cat_id second_cat_id,
   dg.second_cat_name second_cat_name,
   round(nvl(t1.clk_cnt / t1.expre_cnt, 0), 4) ctr,
   round(nvl(t1.gmv / t1.click_uv * t1.clk_cnt / t1.expre_cnt * 10000, 0), 4) gcr,
   round(nvl(t1.ord_cnt / t1.click_uv, 0), 4) rate
 from
 (
   SELECT
     cb.goods_id,
     cb.region_id,
     cb.expre_cnt,
     cb.clk_cnt,
     cb.ord_cnt,
     cb.gmv,
     cb.sales_vol,
     cb.expre_uv as users,
     cb.click_uv
   FROM
    dwd.dwd_vova_activity_goods_ctry_behave cb
  WHERE
       cb.pt = '${cur_date}'
       and cb.first_cat_id = 5712
       AND cb.region_id IS NOT NULL
       AND cb.region_id IN (0,4003,4056,4017,4143,3858)
       and cb.is_brand =0
 ) t1
 left join
   dim.dim_vova_goods dg
 on t1.goods_id = dg.goods_id
)

INSERT overwrite TABLE ads.ads_vova_activity_home_garden PARTITION ( pt = '${cur_date}' )
select
/*+ REPARTITION(1) */
  goods_id,
  region_id,
  first_cat_id,
  nvl(second_cat_id, 0) second_cat_id,
  rule_id biz_type,
  '3' rp_type,
  row_number() over(PARTITION BY rule_id ORDER BY gcr DESC) rank
from
(
  select
    goods_id,
    region_id,
    first_cat_id,
    second_cat_id,
    'jiaju-topsale' rule_id,
    gcr
  from
    tmp_home_garden
  where (region_id=4003 and expre_cnt < 10000 and expre_cnt > 500 and ctr > 0.02 and gcr > 100)
    or (region_id=4056 and expre_cnt < 10000 and expre_cnt > 500 and ctr > 0.02 and gcr > 60)
    or (region_id=4017 and expre_cnt < 10000 and expre_cnt > 500 and ctr > 0.02 and gcr > 60)
    or (region_id=3858 and expre_cnt < 10000 and expre_cnt > 300 and ctr > 0.02 and rate > 0.03 and gmv > 2)
    or (region_id=4143 and expre_cnt < 10000 and expre_cnt > 200 and ctr > 0.02 and rate > 0.03 and gmv > 2)
    or (region_id=0 and expre_cnt < 10000 and expre_cnt > 3000 and ctr > 0.02 and gcr > 60)

  union all
  select
  *
  from (
    select
      goods_id,
      region_id,
      first_cat_id,
      second_cat_id,
      case when second_cat_name = 'Lights & Lighting' and expre_cnt < 10000 and expre_cnt > 500 and ctr > 0.02 and rate > 0.02 then 'jiaju-lights'
        when second_cat_name = 'Home Textile' and expre_cnt < 10000 and expre_cnt > 500 and ctr > 0.02 and gcr > 60 then 'jiaju-hometextile'
        when second_cat_name = 'Home Decor' and expre_cnt < 10000 and expre_cnt > 500 and ctr > 0.02 and gcr > 60 then 'jiaju-homedecor'
        else 'not match'
      end rule_id,
      gcr
    from
      tmp_home_garden
    where region_id = 0 and second_cat_name in ('Lights & Lighting','Home Textile','Home Decor')
  ) where rule_id != 'not match'

  union all
  select
    goods_id,
    region_id,
    first_cat_id,
    second_cat_id,
    'jiaju-newarrivals' rule_id,
    gcr
  from
    tmp_home_garden
  where
    region_id = 0 and expre_cnt < 3000 and expre_cnt > 100 and ctr > 0.02 and ord_cnt > 1 and gcr > 60

  union all
  select
    goods_id,
    region_id,
    first_cat_id,
    second_cat_id,
    'jiaju-moretolove' rule_id,
    gcr
  from
    tmp_home_garden
  where
    (region_id in (4003,4056,4017) and clk_cnt < 100 and clk_cnt > 10 and ctr > 0.02 and rate > 0.02 and gcr > 100)
    or (region_id in (3858,4143) and clk_cnt < 100 and clk_cnt > 5 and ctr > 0.02 and rate > 0.02 and gcr > 40)
    or (region_id = 0 and clk_cnt < 100 and clk_cnt > 20 and ctr > 0.02 and rate > 0.02 and gcr > 60)
)
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism=300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=120" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

