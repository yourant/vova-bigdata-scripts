#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1

#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "cur_date: ${cur_date}"

job_name="mlb_vova_gender_hot_goods_d_req9617_heliu_chenkai"

###逻辑sql
sql="
insert overwrite table mlb.mlb_vova_gender_hot_goods_d partition(pt='${cur_date}')
select
  gender,
  goods_id,
  overall_cat_score
from
(
  select
    tmp1.gender,
    tmp1.goods_id,
    mcs.overall_cat_score,
    row_number() over(partition by gender, second_cat_id order by overall_cat_score desc) row 
  from
  (
    select
      1 gender,
      goods_id,
      second_cat_id
    from 
      ads.ads_vova_goods_portrait
    where pt='${cur_date}'
      and is_recommend = 1
      and second_cat_id in (5784,5789,5773,5793,5721,5795,5786,5785,5841,5830,5839,5732,5834,6375,5832,5733,5794,5836,5831,
    5835,5787,5833,5792,5891,5731,5737,5790,5975,5890,5974,5966,5838,5736,5892,5897,5837,5894,5893)
    
    union all
    select
      0 gender,
      goods_id,
      second_cat_id
    from
      ads.ads_vova_goods_portrait
    where pt='${cur_date}'
      and is_recommend = 1
      and second_cat_id in (5741,6374,165,5781,5903,5902,5799,5905,195,5796,5939,5904,5962,5954,3001,5963,5909,5907,5797,5800,5810,
    164,3004,5798,171,5811,5944,166,5928,5807,5825,173,5812,5927,5930,5960,5814,6008,6009,5813,5929,6007,
    5906,5967,5932,5808,5788,5805,5815,5961,6006,5911,5988,5816,5934,5803,5931,5933,5935)
    
    union all
    select
      -1 gender, -- 通用
      goods_id,
      second_cat_id
    from
      ads.ads_vova_goods_portrait
    where pt='${cur_date}'
      and is_recommend = 1
      and brand_id=0
      and second_cat_id in (5940,5964,5823,5883,5994,5991,5711,5990,5992,5993,5978,5775,5717,5721,5718,5803,5941,5735,5804,5721 ,5718  ,6025 ,5804 ,6000)
  ) tmp1 
  left join
    mlb.mlb_vova_rec_b_catgoods_score_d mcs
  on tmp1.goods_id = mcs.goods_id 
  where mcs.pt = '${cur_date}'
) t1
where row <= 50
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


