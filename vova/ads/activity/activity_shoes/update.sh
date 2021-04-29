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

job_name="ads_vova_activity_shoes_req9345_chenkai_${cur_date}"

###逻辑sql
sql="
insert overwrite table ads.ads_vova_activity_shoes partition(pt='${cur_date}')
select /*+ REPARTITION(1) */
  cb.goods_id goods_id,
  cb.region_id region_id,
  'shoes_best' biz_type,
  3 rp_type,
  cb.first_cat_id first_cat_id,
  nvl(cb.second_cat_id, 0) second_cat_id,
  row_number() over(partition by region_id ORDER BY gmv / click_uv * clk_cnt / expre_cnt * 10000 desc) rank
from
  dwd.dwd_vova_activity_goods_ctry_behave cb
where cb.pt = '${cur_date}'
  and cb.is_brand = 0
  and cb.first_cat_id = 5777
  and (
    (
      cb.region_id in (3858, 4003, 4017, 4056, 4143) 
      and expre_cnt >= 400
      and clk_cnt / expre_cnt >= 0.013
      and ord_cnt / click_uv >= 0.013
      and gmv >= 30
      and gmv / click_uv * clk_cnt / expre_cnt * 10000 >= 60
    )
    or 
    (
      cb.region_id = 0 
      and expre_cnt >= 500
      and clk_cnt / expre_cnt >= 0.015
      and ord_cnt / click_uv >= 0.015
      and gmv >= 50
      and gmv / click_uv * clk_cnt / expre_cnt * 10000 >= 60
    )
  ) 

union all
select /*+ REPARTITION(1) */
  cb.goods_id goods_id,
  cb.region_id region_id,
  'shoes_new' biz_type,
  3 rp_type,
  cb.first_cat_id first_cat_id,
  nvl(cb.second_cat_id, 0) second_cat_id,
  row_number() over(partition by region_id ORDER BY gmv / click_uv * clk_cnt / expre_cnt * 10000 desc) rank
from
  dwd.dwd_vova_activity_goods_ctry_behave cb
where cb.pt = '${cur_date}'
  and cb.is_brand = 0
  and cb.first_cat_id = 5777
  and (
    (
      cb.region_id in (3858, 4003, 4017, 4056, 4143) 
      and expre_cnt >= 100 and expre_cnt <= 5000
      and clk_cnt / expre_cnt >= 0.02
      and ord_cnt / click_uv >= 0.015
      and sales_vol >= 2
      and gmv / click_uv * clk_cnt / expre_cnt * 10000 >= 100
    )
    or 
    (
      cb.region_id = 0 
      and expre_cnt >= 100 and expre_cnt <= 10000
      and clk_cnt / expre_cnt >= 0.025
      and ord_cnt / click_uv >= 0.03
      and sales_vol >= 2
      and gmv / click_uv * clk_cnt / expre_cnt * 10000 >= 100
    )
  ) 

union all
select /*+ REPARTITION(1) */
  cb.goods_id goods_id,
  cb.region_id region_id,
  'shoes_athletic' biz_type,
  3 rp_type,
  cb.first_cat_id first_cat_id,
  nvl(cb.second_cat_id, 0) second_cat_id,
  row_number() over(partition by region_id ORDER BY gmv / click_uv * clk_cnt / expre_cnt * 10000 desc) rank
from
  dwd.dwd_vova_activity_goods_ctry_behave cb
where cb.pt = '${cur_date}'
  and cb.is_brand = 0
  and cb.first_cat_id = 5777
  and cb.second_cat_id = 5883
  and (
    (
      cb.region_id in (3858, 4003, 4017, 4056, 4143) 
      and expre_cnt >= 100 
      and clk_cnt / expre_cnt >= 0.013
      and ord_cnt / click_uv >= 0.01
      and gmv >= 15
      and gmv / click_uv * clk_cnt / expre_cnt * 10000 >= 100
    )
    or 
    (
      cb.region_id = 0 
      and expre_cnt >= 200 
      and clk_cnt / expre_cnt >= 0.015
      and ord_cnt / click_uv >= 0.015
      and gmv >= 20
      and gmv / click_uv * clk_cnt / expre_cnt * 10000 >= 120
    )
  ) 

union all
select /*+ REPARTITION(1) */
  cb.goods_id goods_id,
  cb.region_id region_id,
  'shoes_casual' biz_type,
  3 rp_type,
  cb.first_cat_id first_cat_id,
  nvl(cb.second_cat_id, 0) second_cat_id,
  row_number() over(partition by region_id ORDER BY gmv / click_uv * clk_cnt / expre_cnt * 10000 desc) rank
from
  dwd.dwd_vova_activity_goods_ctry_behave cb
where cb.pt = '${cur_date}'
  and cb.is_brand = 0
  and cb.first_cat_id = 5777
  and cb.second_cat_id = 5971
  and (
    (
      cb.region_id in (3858, 4003, 4017, 4056, 4143) 
      and expre_cnt >= 100 
      and clk_cnt / expre_cnt >= 0.013
      and ord_cnt / click_uv >= 0.01
      and gmv >= 15
      and gmv / click_uv * clk_cnt / expre_cnt * 10000 >= 80
    )
    or 
    (
      cb.region_id = 0 
      and expre_cnt >= 200 
      and clk_cnt / expre_cnt >= 0.015
      and ord_cnt / click_uv >= 0.015
      and gmv >= 20
      and gmv / click_uv * clk_cnt / expre_cnt * 10000 >= 100
    )
  ) 

union all
select /*+ REPARTITION(1) */
  cb.goods_id goods_id,
  cb.region_id region_id,
  'shoes_sandals' biz_type,
  3 rp_type,
  cb.first_cat_id first_cat_id,
  nvl(cb.second_cat_id, 0) second_cat_id,
  row_number() over(partition by region_id ORDER BY gmv / click_uv * clk_cnt / expre_cnt * 10000 desc) rank
from
  dwd.dwd_vova_activity_goods_ctry_behave cb
where cb.pt = '${cur_date}'
  and cb.is_brand = 0
  and cb.first_cat_id = 5777
  and cb.second_cat_id = 5972
  and (
    (
      cb.region_id in (3858, 4003, 4017, 4056, 4143) 
      and expre_cnt >= 50 
      and clk_cnt / expre_cnt >= 0.01
      and ord_cnt / click_uv >= 0.01
      and gmv >= 10
      and gmv / click_uv * clk_cnt / expre_cnt * 10000 >= 60
    )
    or 
    (
      cb.region_id = 0 
      and expre_cnt >= 200 
      and clk_cnt / expre_cnt >= 0.01
      and ord_cnt / click_uv >= 0.01
      and gmv >= 10
      and gmv / click_uv * clk_cnt / expre_cnt * 10000 >= 80
    )
  ) 

union all
select /*+ REPARTITION(1) */
  cb.goods_id goods_id,
  cb.region_id region_id,
  'shoes_slippers' biz_type,
  3 rp_type,
  cb.first_cat_id first_cat_id,
  nvl(cb.second_cat_id, 0) second_cat_id,
  row_number() over(partition by region_id ORDER BY gmv / click_uv * clk_cnt / expre_cnt * 10000 desc) rank
from
  dwd.dwd_vova_activity_goods_ctry_behave cb
where cb.pt = '${cur_date}'
  and cb.is_brand = 0
  and cb.first_cat_id = 5777
  and cb.second_cat_id = 5970
  and (
    (
      cb.region_id in (3858, 4003, 4017, 4056, 4143) 
      and expre_cnt >= 50 
      and clk_cnt / expre_cnt >= 0.01
      and ord_cnt / click_uv >= 0.01
      and gmv >= 10
      and gmv / click_uv * clk_cnt / expre_cnt * 10000 >= 60
    )
    or 
    (
      cb.region_id = 0 
      and expre_cnt >= 200 
      and clk_cnt / expre_cnt >= 0.01
      and ord_cnt / click_uv >= 0.01
      and gmv >= 10
      and gmv / click_uv * clk_cnt / expre_cnt * 10000 >= 80
    )
  ) 

union all
select /*+ REPARTITION(1) */
  cb.goods_id goods_id,
  cb.region_id region_id,
  'shoes_fashion' biz_type,
  3 rp_type,
  cb.first_cat_id first_cat_id,
  nvl(cb.second_cat_id, 0) second_cat_id,
  row_number() over(partition by region_id ORDER BY gmv / click_uv * clk_cnt / expre_cnt * 10000 desc) rank
from
  dwd.dwd_vova_activity_goods_ctry_behave cb
where cb.pt = '${cur_date}'
  and cb.is_brand = 0
  and cb.first_cat_id = 5777
  and cb.second_cat_id = 5969
  and (
    (
      cb.region_id in (3858, 4003, 4017, 4056, 4143) 
      and expre_cnt >= 50 
      and clk_cnt / expre_cnt >= 0.01
      and ord_cnt / click_uv >= 0.01
      and gmv >= 10
      and gmv / click_uv * clk_cnt / expre_cnt * 10000 >= 60
    )
    or 
    (
      cb.region_id = 0 
      and expre_cnt >= 200 
      and clk_cnt / expre_cnt >= 0.01
      and ord_cnt / click_uv >= 0.01
      and gmv >= 10
      and gmv / click_uv * clk_cnt / expre_cnt * 10000 >= 80
    )
  ) 

union all
select /*+ REPARTITION(1) */
  cb.goods_id goods_id,
  cb.region_id region_id,
  'shoes_boots' biz_type,
  3 rp_type,
  cb.first_cat_id first_cat_id,
  nvl(cb.second_cat_id, 0) second_cat_id,
  row_number() over(partition by region_id ORDER BY gmv / click_uv * clk_cnt / expre_cnt * 10000 desc) rank
from
  dwd.dwd_vova_activity_goods_ctry_behave cb
where cb.pt = '${cur_date}'
  and cb.is_brand = 0
  and cb.first_cat_id = 5777
  and cb.second_cat_id = 5968
  and (
    (
      cb.region_id in (3858, 4003, 4017, 4056, 4143) 
      and expre_cnt >= 50 
      and clk_cnt / expre_cnt >= 0.01
      and ord_cnt / click_uv >= 0.01
      and gmv >= 10
      and gmv / click_uv * clk_cnt / expre_cnt * 10000 >= 60
    )
    or 
    (
      cb.region_id = 0 
      and expre_cnt >= 200 
      and clk_cnt / expre_cnt >= 0.01
      and ord_cnt / click_uv >= 0.01
      and gmv >= 10
      and gmv / click_uv * clk_cnt / expre_cnt * 10000 >= 80
    )
  ) 
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
