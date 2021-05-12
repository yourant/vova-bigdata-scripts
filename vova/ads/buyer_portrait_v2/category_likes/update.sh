#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
ALTER TABLE ads.ads_vova_buyer_portrait_category_likes DROP if exists partition(pt = '$(date -d "${pre_date:0:10} -180day" +%Y-%m-%d)');
insert overwrite table ads.ads_vova_buyer_portrait_category_likes partition(pt='$pre_date')
SELECT
  /*+ REPARTITION(30) */
  buyer_id,
  cat_id,
  sum( IF ( day_gap < 7, expre_cnt, 0 ) ) AS expre_cnt_1w,
  sum( IF ( day_gap < 15, expre_cnt, 0 ) ) AS expre_cnt_15d,
  sum( IF ( day_gap < 30, expre_cnt, 0 ) ) AS expre_cnt_1m,
  sum( IF ( day_gap < 7, clk_cnt, 0 ) ) AS clk_cnt_1w,
  sum( IF ( day_gap < 15, clk_cnt, 0 ) ) AS clk_cnt_15d,
  sum( IF ( day_gap < 30, clk_cnt, 0 ) ) AS clk_cnt_1m,
  sum( IF ( day_gap < 7, clk_valid_cnt, 0 ) ) AS clk_valid_cnt_1w,
  sum( IF ( day_gap < 15, clk_valid_cnt, 0 ) ) AS clk_valid_cnt_15d,
  sum( IF ( day_gap < 30, clk_valid_cnt, 0 ) ) AS clk_valid_cnt_1m,
  sum( IF ( day_gap < 7, collect_cnt, 0 ) ) AS collect_cnt_1w,
  sum( IF ( day_gap < 15, collect_cnt, 0 ) ) AS collect_cnt_15d,
  sum( IF ( day_gap < 30, collect_cnt, 0 ) ) AS collect_cnt_1m,
  sum( IF ( day_gap < 7, add_cat_cnt, 0 ) ) AS add_cat_cnt_1w,
  sum( IF ( day_gap < 15, add_cat_cnt, 0 ) ) AS add_cat_cnt_15d,
  sum( IF ( day_gap < 30, add_cat_cnt, 0 ) ) AS add_cat_cnt_1m,
  sum( IF ( day_gap < 7, ord_cnt, 0 ) ) AS ord_cnt_1w,
  sum( IF ( day_gap < 15, ord_cnt, 0 ) ) AS ord_cnt_15d,
  sum( IF ( day_gap < 30, ord_cnt, 0 ) ) AS ord_cnt_1m,
  sum(IF(day_gap < 7, gmv, 0 ) ) AS gmv_1w,
  sum(IF(day_gap < 15, gmv, 0 ) ) AS gmv_15d,
  sum(IF(day_gap < 30, gmv, 0 ) ) AS gmv_1m,
  min(clk_day_gap) min_clk_day_gap -- 用户最后一次点击该一级品类到现在的时间
FROM
  (
 SELECT
    buyer_id,
    cat_id,
    pt,
    datediff('${pre_date}', pt) AS day_gap,
    if(sum(clk_cnt) > 0, datediff('${pre_date}', pt), null) AS clk_day_gap, -- 点击该一级品类与当天的时间差
    sum(expre_cnt) AS expre_cnt,
    sum(clk_cnt) AS clk_cnt,
    sum(clk_valid_cnt) AS clk_valid_cnt,
    sum(collect_cnt) AS collect_cnt,
    sum(add_cat_cnt) AS add_cat_cnt,
    sum(ord_cnt) AS ord_cnt,
    sum(gmv) AS gmv
  FROM
    dws.dws_vova_buyer_goods_behave
  WHERE
    pt > date_sub('${pre_date}', 30) AND pt <= '${pre_date}' and second_cat_id is not null
  GROUP BY
    buyer_id,
    cat_id,
    pt
  )
GROUP BY
  buyer_id,
  cat_id;
INSERT OVERWRITE TABLE ads.ads_vova_buyer_portrait_category_likes_with_click_15d
SELECT
/*+ REPARTITION(10) */
buyer_id,
cat_id,
expre_cnt_1w,
expre_cnt_15d,
expre_cnt_1m,
clk_cnt_1w,
clk_cnt_15d,
clk_cnt_1m,
clk_valid_cnt_1w,
clk_valid_cnt_15d,
clk_valid_cnt_1m,
collect_cnt_1w,
collect_cnt_15d,
collect_cnt_1m,
add_cat_cnt_1w,
add_cat_cnt_15d,
add_cat_cnt_1m,
ord_cnt_1w,
ord_cnt_15d,
ord_cnt_1m
FROM
ads.ads_vova_buyer_portrait_category_likes
where pt='${pre_date}' and clk_cnt_15d >0;


insert overwrite table ads.ads_vova_buyer_portrait_category_likes_medium_term partition(pt='${pre_date}')
SELECT
  /*+ REPARTITION(15) */
  buyer_id,
  cat_id,
  sum(expre_cnt) AS expre_cnt,
  sum(clk_cnt) AS clk_cnt,
  sum(clk_valid_cnt) AS clk_valid_cnt,
  sum(collect_cnt) AS collect_cnt,
  sum(add_cat_cnt) AS add_cat_cnt,
  sum(ord_cnt) AS ord_cnt,
  sum(gmv) AS gmv,
  min(clk_day_gap) min_clk_day_gap -- 用户最后一次点击该一级品类到现在的时间
FROM
(
  SELECT
    buyer_id,
    cat_id,
    pt,
    if(sum(clk_cnt) > 0, datediff('${pre_date}', pt), null) AS clk_day_gap, -- 点击该一级品类与当天的时间差
    sum(expre_cnt) AS expre_cnt,
    sum(clk_cnt) AS clk_cnt,
    sum(clk_valid_cnt) AS clk_valid_cnt,
    sum(collect_cnt) AS collect_cnt,
    sum(add_cat_cnt) AS add_cat_cnt,
    sum(ord_cnt) AS ord_cnt,
    sum(gmv) AS gmv
  FROM
    dws.dws_vova_buyer_goods_behave
  WHERE
    pt >= date_sub('${pre_date}', 60) AND pt < date_sub('${pre_date}', 15) and cat_id is not null
  GROUP BY
    buyer_id,
    cat_id,
    pt
) t
GROUP BY
  buyer_id,
  cat_id
;

insert overwrite table ads.ads_vova_buyer_portrait_category_likes_long_term partition(pt='${pre_date}')
select
/*+ REPARTITION(1) */
  fp.buyer_id buyer_id,
  dg.cat_id cat_id,
  count(distinct(fp.order_goods_id)) AS ord_cnt,
  sum(fp.shop_price * fp.goods_number + fp.shipping_fee) AS gmv
from
  dwd.dwd_vova_fact_pay fp
left join
  dim.dim_vova_goods dg
on fp.datasource = dg.datasource and fp.goods_id = dg.goods_id
where fp.confirm_time < date_sub('${pre_date}', 60) and dg.second_cat_id is not null
group by
  fp.buyer_id,
  dg.cat_id
;

insert overwrite table ads.ads_vova_buyer_portrait_category_likes_exp partition(pt='${pre_date}')
select
/*+ REPARTITION(15) */
  buyer_id,
  cat_id,
  weights,
  log(14,if(weights_short*1000>195,195,weights_short*1000)+1)/2,
  log(7,if(weights_medium*1000>48,48,weights_medium*1000)+1)/2,
  log(14,if(weights_long*1000>195,195,weights_long*1000)+1)/2,
  log(14,if((0.6 * weights_short+ 0.3 * weights_medium + 0.1 * weights_long)*10000>195,195,(0.6 * weights_short+ 0.3 * weights_medium + 0.1 * weights_long)*10000)+1)/2 -- 综合偏好
from
(
  select
    buyer_id,
    cat_id,
    nvl(first(weights),0) weights,
    nvl(first(weights_short),0) weights_short, -- 短期偏好度
    nvl(first(weights_medium),0) weights_medium, -- 中期偏好度
    nvl(first(weights_long),0) weights_long -- 长期偏好度
  from
  (
    select
      r.buyer_id,
      l.cat_id,
      exp(- 0.076 * nvl(l.min_clk_day_gap, 31)) * (
        0.3 * ( ord_cnt_1m / ( ord_cnt_all + 1 ) )
        + 0.1 * ( clk_valid_cnt_1m / ( clk_valid_cnt_all + 1 ) )
        + 0.3 * ( collect_cnt_1m / ( collect_cnt_all + 1 ) )
        + 0.4 * (add_cat_cnt_1m / ( add_cat_cnt_all + 1 ) )
        ) weights,
      exp(-0.076 * nvl(l.min_clk_day_gap, 31)) * (0.3 * (ord_cnt_15d / (ord_cnt_15d_all + 1))
        + 0.1 * (clk_valid_cnt_15d / (clk_valid_cnt_15d_all + 1))
        + 0.3 * (collect_cnt_15d / (collect_cnt_15d_all + 1))
        + 0.4 * (add_cat_cnt_15d / (add_cat_cnt_15d_all + 1))
        ) weights_short, -- 短期偏好度
      null weights_medium, -- 中期偏好度
      null weights_long -- 长期偏好度
    from
    (
      select
        buyer_id,
        sum(ord_cnt_1m) ord_cnt_all,
        sum(clk_cnt_1m) clk_cnt_all,
        sum(clk_valid_cnt_1m) clk_valid_cnt_all,
        sum(collect_cnt_1m) collect_cnt_all,
        sum(add_cat_cnt_1m) add_cat_cnt_all,
        sum(gmv_1m) gmv_all,

        sum(ord_cnt_15d) ord_cnt_15d_all,
        sum(clk_cnt_15d) clk_cnt_15d_all,
        sum(clk_valid_cnt_15d) clk_valid_cnt_15d_all,
        sum(collect_cnt_15d) collect_cnt_15d_all,
        sum(add_cat_cnt_15d) add_cat_cnt_15d_all,
        sum(gmv_15d) gmv_15d_all
      from ads.ads_vova_buyer_portrait_category_likes
      where pt='${pre_date}' and min_clk_day_gap is not null
      GROUP by buyer_id
    ) r
    left join
    (select *
      from ads.ads_vova_buyer_portrait_category_likes
      where pt='${pre_date}' and buyer_id>0 and min_clk_day_gap is not null
    ) l
    on l.buyer_id = r.buyer_id
  union all
    select
      r.buyer_id,
      l.cat_id,
      null weights,
      null weights_short, -- 短期偏好度
      exp(-0.0315 * if(nvl(l.min_clk_day_gap, 61)-15 < 0, 0, nvl(l.min_clk_day_gap, 61)-15)) * (0.4 * (l.ord_cnt / (ord_cnt_medium_term_all + 1))
        + 0.1 * (l.clk_cnt / (clk_cnt_medium_term_all + 1))
        + 0.3 * (l.collect_cnt / (collect_cnt_medium_term_all + 1))
        + 0.1 * (l.add_cat_cnt / (add_cat_cnt_medium_term_all + 1))
        ) weights_medium, -- 中期偏好度
      null weights_long -- 长期偏好度
    from
    (
      select
        buyer_id,
        sum(ord_cnt) ord_cnt_medium_term_all,
        sum(clk_cnt) clk_cnt_medium_term_all,
        sum(collect_cnt) collect_cnt_medium_term_all,
        sum(add_cat_cnt) add_cat_cnt_medium_term_all
      from ads.ads_vova_buyer_portrait_category_likes_medium_term
      where pt='${pre_date}' and min_clk_day_gap is not null
      GROUP by buyer_id
    ) r
    left join
    (
      select *
      from ads.ads_vova_buyer_portrait_category_likes_medium_term
      where pt='${pre_date}' and buyer_id>0 and min_clk_day_gap is not null
    ) l
    on l.buyer_id = r.buyer_id
  union all
    select
      r.buyer_id,
      l.cat_id,
      null weights,
      null weights_short, -- 短期偏好度
      null weights_medium, -- 中期偏好度
      0.5 * (0.7 * (l.ord_cnt / (ord_cnt_long_term_all + 1))
        + 0.3 * (l.gmv / (gmv_long_term_all + 1))
        ) weights_long -- 长期偏好度
    from
    (
      select
        buyer_id,
        sum(ord_cnt) ord_cnt_long_term_all,
        sum(gmv) gmv_long_term_all
      from ads.ads_vova_buyer_portrait_category_likes_long_term
      where pt='${pre_date}' and gmv > 0
      GROUP by buyer_id
    ) r
    left join
    (
      select *
      from ads.ads_vova_buyer_portrait_category_likes_long_term
      where pt='${pre_date}' and buyer_id > 0 and gmv > 0
    ) l
    on l.buyer_id = r.buyer_id
  ) tmp where buyer_id > 0 and cat_id is not null
    group by buyer_id, cat_id
) result
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_buyer_portrait_category_likes" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
