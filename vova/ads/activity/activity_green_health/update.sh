#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  cur_date=$(date -d "-1 day" +%Y-%m-%d)
fi
echo "cur_date: ${cur_date}"

sql="
with tmp_green_health_goods_id as (
select distinct goods_id
from (
select goods_id
from dwb.dwb_vova_red_packet_goods
where pt = '${cur_date}'
and gsn_status = 3
union all
select goods_id
from tmp.ttt_ysj
) tmp
)

insert overwrite table ads.ads_vova_activity_green_health partition(pt='${cur_date}')
select *
from (
select
goods_id,
region_id,
biz_type,
rp_type,
first_cat_id,
second_cat_id,
row_number() over(partition by biz_type ORDER BY ord_cnt/expre_cnt * 10000 desc) rank
from (
select
  cb.goods_id goods_id,
  cb.region_id region_id,
  case
  when cb.first_cat_id = 5768 then '2021lvyin_Men'
  when cb.first_cat_id = 5777 then '2021lvyin_Shoes'
  when cb.first_cat_id = 194 then '2021lvyin_Women'
  when cb.first_cat_id = 5715 then '2021lvyin_Bags'
  when cb.first_cat_id = 5771 then '2021lvyin_Sports'
  when cb.first_cat_id = 5769 then '2021lvyin_Beauty'
  when cb.first_cat_id = 5713 or cb.first_cat_id = 5976 then '2021lvyin_Electronics'
  when cb.first_cat_id = 5712 then '2021lvyin_Home'
  when cb.first_cat_id = 5770 or cb.first_cat_id = 5809 then '2021lvyin_Baby'
  end as biz_type,
  3 rp_type,
  cb.first_cat_id first_cat_id,
  nvl(cb.second_cat_id, 0) second_cat_id,
  ord_cnt,
  expre_cnt
from dwd.dwd_vova_activity_goods_ctry_behave cb
inner join tmp_green_health_goods_id g
on cb.goods_id = g.goods_id
and cb.pt = '${cur_date}'
and cb.region_id in (3858, 4003, 4017, 4056, 4143,0)
) tmp
where biz_type is not null
UNION ALL
select
  cb.goods_id goods_id,
  cb.region_id region_id,
  '2021lvyin_ALL' biz_type,
  3 rp_type,
  cb.first_cat_id first_cat_id,
  nvl(cb.second_cat_id, 0) second_cat_id,
  row_number() over (ORDER BY ord_cnt/expre_cnt * 10000 desc) rank
from dwd.dwd_vova_activity_goods_ctry_behave cb
inner join tmp_green_health_goods_id g
on cb.goods_id = g.goods_id
and cb.pt = '${cur_date}'
and cb.region_id in (3858, 4003, 4017, 4056, 4143,0)
and cb.first_cat_id in (5768,5777,194,5715,5771,5769,5713,5976,5712,5770,5809)
) tt
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=ads_vova_activity_green_house" \
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