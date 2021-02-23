#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
insert overwrite table ads.ads_vova_buyer_stat_feature
select
pf.buyer_id,
pf.reg_gender,
pf.reg_age_group,
pf.reg_ctry,
pf.reg_time,
pf.reg_channel,
pf.os_type,
tmp_first_cat.likesStr as first_cat_likes,
tmp_second_cat.likesStr  as second_cat_likes,
if(date(pf.first_order_time)<'2000-01-01','2000-01-01',pf.first_order_time) as first_order_time,
pf.last_order_time,
pf.order_cnt,
pf.avg_price,
nvl(tmp_max_price_likes.price_range,0) as price_range,
pf.buyer_act,
pf.trade_act,
pf.last_logint_type,
pf.last_buyer_type,
pf.buy_times_type,
email_act
from
ads.ads_vova_buyer_portrait_feature pf
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
    concat_ws( ',', collect_list ( first_cat_id ) ) AS likesStr
FROM
    (
SELECT
    fc.buyer_id,
    fc.likes_weight_synth,
    fc.first_cat_id,
    row_number ( ) over ( PARTITION BY fc.buyer_id ORDER BY fc.likes_weight_synth DESC ) AS rk
FROM
    ads.ads_vova_buyer_portrait_first_category_likes_exp fc
WHERE
    fc.pt = '${pre_date}'
    )
WHERE
    rk <= 3
GROUP BY
    buyer_id
) tmp_first_cat
on pf.buyer_id = tmp_first_cat.buyer_id

left join
(
  --二级品类
  select
    buyer_id,
    concat_ws( ',', collect_list ( second_cat_id ) ) AS likesStr
  from
  (
    select
      fc.buyer_id,
      fc.second_cat_id,
      row_number() over(partition by fc.buyer_id order by fc.likes_weight_synth desc) as rk
    from
      ads.ads_vova_buyer_portrait_second_category_likes_exp fc
    where fc.pt='${pre_date}'
  )
  where rk<=3
  group by buyer_id
) tmp_second_cat on pf.buyer_id = tmp_second_cat.buyer_id


where pf.pt='${pre_date}'
"




#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
    --driver-memory 4G \
    --executor-memory 4G --executor-cores 1 \
    --conf "spark.sql.parquet.writeLegacyFormat=true"  \
    --conf "spark.dynamicAllocation.minExecutors=30" \
    --conf "spark.dynamicAllocation.initialExecutors=30" \
    --conf "spark.app.name=ads_vova_buyer_stat_feature" \
    --conf "spark.sql.crossJoin.enabled=true" \
    --conf "spark.default.parallelism = 360" \
    --conf "spark.sql.shuffle.partitions=360" \
    --conf "spark.dynamicAllocation.maxExecutors=250" \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.sql.adaptive.join.enabled=true" \
    --conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
    -e "${sql}"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
