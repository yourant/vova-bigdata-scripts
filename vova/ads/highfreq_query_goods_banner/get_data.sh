#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1

#默认日期为昨天
if [ ! -n "$1" ];then
  cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
# 当天日期
echo "cur_date: ${cur_date}"

table_suffix=`date -d "${cur_date}" +%Y%m%d`
echo "table_suffix: ${table_suffix}"

job_name="ads_vova_highfreq_query_goods_banner_get_data_req9485_chenkai_${cur_date}"

###逻辑sql
# 取数: 相似图片组中历史有销量的非brand商品
sql="
add jar hdfs:///tmp/jar/base64_to_long_udtf.jar;
CREATE TEMPORARY FUNCTION Base64ToLongUDTF as 'com.vova.bigdata.sparkbatch.utils.Base64ToLongUDTF';

insert overwrite table ads.ads_vova_highfreq_query_goods_banner partition(pt='${cur_date}')
select /*+ REPARTITION(1) */
  hqm.source_origin, -- 原始query
  t1.query, -- 归并映射mapping之后的query
  t1.goods_id, -- 商品ID
  aim.img_id img_id,
  aim.url img_url,
  dg.cat_id,
  dg.brand_id,
  ovb.brand_name,
  bnt.bod_id,
  bnt.bod_name_translation,
  bnt.language_id
from
(
  select
    t1.query,
    t1.goods_id
  from
  (
    select
      split(query_keys, '@@@')[0] query,
      Base64ToLongUDTF(goods_list) goods_id
    from
      mlb.mlb_vova_highfreq_query_match_d
    where pt ='${cur_date}'
  ) t1
  left join
    mlb.mlb_vova_rec_b_catgoods_score_d t2
  on t1.goods_id = t2.goods_id
  where t2.pt='${cur_date}'
    and t2.overall_cat_score > 30
) t1 -- 一级品类评分 大于 30 分的热搜词商品
inner join
  dim.dim_vova_goods dg
on t1.goods_id = dg.goods_id
left join
  ods_vova_vts.ods_vova_brand ovb
on dg.brand_id = ovb.brand_id
left join
(
  select
    *
  from
  (
    select
      *,
      row_number() over(partition by goods_id, img_id order by pt desc) rank
    from
      ads.ads_vova_image_matting
  ) im
  where rank = 1
) aim
on t1.goods_id = aim.goods_id
left join
(
  select *
  from
    mlb.mlb_vova_highfreq_query_mapping_d
  where pt='${cur_date}'
) hqm
on t1.query = hqm.target_query
left join
  ads.ads_vova_bod_name_translation bnt
on t1.query = bnt.bod_name
left join
(
  select
    goods_id,
    img_id
  from
    ads.ads_vova_highfreq_query_goods_banner
  where pt < '${cur_date}'
) tmp_hqgb
on tmp_hqgb.goods_id = t1.goods_id and tmp_hqgb.img_id = aim.img_id
where aim.img_id is not null
  and bnt.bod_name is not null
  and tmp_hqgb.img_id is null
;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism=300" \
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
