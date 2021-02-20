#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ads.ads_vova_buyer_page_tag partition(bpt)
select
/*+ REPARTITION(3) */
pf.buyer_id,
pf.datasource,
db.current_device_id as device_id,
db.email,
pf.reg_gender,
pf.reg_age_group,
pf.reg_time,
pf.reg_ctry,
vl.code,
pf.reg_channel,
pf.os_type,
pf.first_order_time,
pf.last_order_time,
pf.order_cnt,
pf.avg_price,
tmp_max_price_likes.price_range as price_range_type,
pf.buy_times_type,
pf.reg_tag,
pf.buyer_act,
pf.trade_act,
tmp_first_cat.likesStr as first_cat_likes,
tmp_second_cat.likesStr as second_cat_likes,
null as second_cat_style_likes,
tmp_second_cat_word.scoreStr as second_cat_key_word_likes,
tmp_second_cat_price.likesStr as second_cat_price_likes,
tmp_brand.likesStr as brand_likes,
tmp_search_word.likesStr as searchs,
cast(substr(pf.buyer_id,4) as int)%200 as bpt
from
ads.ads_vova_buyer_portrait_feature pf
left join dim.dim_vova_buyers db on  pf.buyer_id = db.buyer_id
left join ods_vova_vts.ods_vova_languages vl on pf.lag_id = vl.languages_id
left join
(
--价格区间偏好
SELECT
buyer_id,
price_range
FROM
    (
SELECT
    buyer_id,
    price_range,
    row_number ( ) over ( PARTITION BY buyer_id ORDER BY likes_weight DESC ) rk
FROM
    ( SELECT buyer_id, price_range, sum( likes_weight ) likes_weight FROM ads.ads_vova_buyer_portrait_second_category_likes_price_range_exp where pt = '${pre_date}' GROUP BY buyer_id, price_range )
    )
WHERE
    rk =1
)tmp_max_price_likes

on pf.buyer_id = tmp_max_price_likes.buyer_id
left join
(
--一级品类
SELECT
    buyer_id,
    concat_ws( ' || ', collect_list ( concat( cat_name, '(', likes_weight_synth, ')' ) ) ) AS likesStr
FROM
    (
SELECT
    fc.buyer_id,
    vc.cat_name,
    fc.likes_weight_synth,
    row_number ( ) over ( PARTITION BY fc.buyer_id ORDER BY fc.likes_weight_synth DESC ) AS rk
FROM
    ads.ads_vova_buyer_portrait_first_category_likes_exp fc
    LEFT JOIN ods_vova_vts.ods_vova_category vc ON fc.first_cat_id = vc.cat_id
WHERE
    fc.pt = '${pre_date}'
    AND fc.likes_weight_synth >= 0.5
    )
WHERE
    rk <= 30
GROUP BY
    buyer_id
) tmp_first_cat on pf.buyer_id = tmp_first_cat.buyer_id
left join
(
--品牌偏好
SELECT
buyer_id,
concat_ws( ' || ', collect_list ( concat( brand_name, '(', likes_weight_synth, ')' ) ) ) AS likesStr
FROM
    (
SELECT
    pb.buyer_id,
    nvl ( vb.brand_name, 'NULL' ) AS brand_name,
    pb.likes_weight_synth,
    row_number ( ) over ( PARTITION BY pb.buyer_id ORDER BY pb.likes_weight_synth DESC ) AS rk
FROM
    ads.ads_vova_buyer_portrait_brand_likes_exp pb
    LEFT JOIN ods_vova_vts.ods_vova_brand vb ON pb.brand_id = vb.brand_id
WHERE
    pb.pt = '${pre_date}'
    AND pb.likes_weight_synth >= 0.5
    )
WHERE
    rk <= 30
GROUP BY
    buyer_id
)tmp_brand on pf.buyer_id = tmp_brand.buyer_id
left join
(
--二级品类+价格区间偏好
SELECT
    buyer_id,
    concat_ws( ' || ', collect_list ( concat( cat_name, '(', price, ')', '(', likes_weight_synth, ')' ) ) ) AS likesStr
FROM
    (
SELECT
    sc.buyer_id,
    vc.cat_name,
    concat( prt.min_val, '-', prt.max_val ) AS price,
    sc.likes_weight_synth,
    row_number ( ) over ( PARTITION BY sc.buyer_id ORDER BY sc.likes_weight_synth DESC ) AS rk
FROM
    ads.ads_vova_buyer_portrait_second_category_likes_price_range_exp sc
    LEFT JOIN ods_vova_vts.ods_vova_category vc ON sc.second_cat_id = vc.cat_id
    LEFT JOIN tmp.tmp_vova_dictionary_price_range_type prt ON sc.price_range = prt.id
WHERE
    sc.pt = '${pre_date}'
    AND sc.likes_weight_synth >= 0.5
    )
WHERE
    rk <= 30
GROUP BY
    buyer_id
)tmp_second_cat_price on pf.buyer_id = tmp_second_cat_price.buyer_id
left join
(
  -- 搜索词
  select
    buyer_id,
    concat_ws(' || ',collect_list(key_word)) as likesStr
  from
  (
    select
      buyer_id,
      key_word,
      row_number() over(partition by buyer_id order by search_time desc) rk
    from
      dwd.dwd_vova_fact_search_word sw
  )
  where rk<=100
  group by buyer_id
)tmp_search_word on pf.buyer_id = tmp_search_word.buyer_id
left join
(
  --二级品类
  select
    buyer_id,
    concat_ws(' || ',collect_list(concat(cat_name,'(',likes_weight_synth,')'))) as likesStr
  from
  (
    select
      fc.buyer_id,
      vc.cat_name,
      fc.likes_weight_synth,
      row_number() over(partition by fc.buyer_id order by fc.likes_weight_synth desc) as rk
    from
      ads.ads_vova_buyer_portrait_second_category_likes_exp fc
    left join
      ods_vova_vts.ods_vova_category vc
    on fc.second_cat_id = vc.cat_id
    where fc.pt='${pre_date}' and fc.likes_weight_synth>=0.5
  )
  where rk<=30
  group by buyer_id
) tmp_second_cat on pf.buyer_id = tmp_second_cat.buyer_id
left join
(
--二级品类+关键词偏好
SELECT
    buyer_id,
    concat_ws( ' || ', collect_list ( concat( cat_name, '(', word, ')', '(', score, ')' ) ) ) AS scoreStr
FROM
    (
SELECT
    ws.buyer_id,
    ws.word,
    ws.score,
    vc.cat_name,
    row_number ( ) over ( PARTITION BY ws.buyer_id ORDER BY ws.score DESC ) AS rk
FROM
    ads.ads_vova_buyer_portrait_second_category_word_score ws
    LEFT JOIN ods_vova_vts.ods_vova_category vc ON ws.second_cat_id = vc.cat_id
WHERE
    ws.pt = '${pre_date}' and ws.score>=0.5
    )
WHERE
    rk <= 30
GROUP BY
    buyer_id
)tmp_second_cat_word on pf.buyer_id = tmp_second_cat_word.buyer_id
where pf.pt='${pre_date}'
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 10G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=20" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=ads_vova_buyer_page_tag" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=300000" \
--conf "spark.network.timeout=300" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi