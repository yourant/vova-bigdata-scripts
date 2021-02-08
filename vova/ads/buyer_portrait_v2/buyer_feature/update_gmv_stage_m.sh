#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  cur_date=`date +%Y-%m-%d`
fi

pre_3month_firstday=`date -d "$cur_date - 3 month" +%Y-%m-01`
month_firstday=`date -d $cur_date +%Y-%m-01`

echo "start time:$pre_3month_firstday,end time:$month_firstday"

sql="
with tmp_pay as (
SELECT
    fp.buyer_id,
    sum( fp.shop_price * fp.goods_number + fp.shipping_fee ) AS gmv
FROM
    dwd.dwd_vova_fact_pay fp
WHERE
    date( pay_time ) >= '${pre_3month_firstday}'
    AND date( pay_time ) < '${month_firstday}'
GROUP BY
    fp.buyer_id
),
tmp_buyer_stage as(
SELECT
    db.region_code,
    sum( gmv ) / count( DISTINCT fp.buyer_id ) AS gmv_div_users
FROM
    tmp_pay fp
    INNER JOIN dim.dim_vova_buyers db ON fp.buyer_id = db.buyer_id
WHERE
    db.region_code IS NOT NULL
GROUP BY
    db.region_code
)

INSERT OVERWRITE TABLE ads.ads_vova_buyer_gmv_stage_3m
SELECT
fp.buyer_id,
db.region_code,
IF(fp.gmv < tmp_buyer_stage.gmv_div_users,1,IF(fp.gmv < 2 * tmp_buyer_stage.gmv_div_users,2,IF(fp.gmv < 3 * tmp_buyer_stage.gmv_div_users, 3, IF ( fp.gmv >= 3 * tmp_buyer_stage.gmv_div_users,4,0 ) ) ) ) AS gmv_stage
FROM
    tmp_pay fp
    INNER JOIN dim.dim_vova_buyers db ON fp.buyer_id = db.buyer_id
    INNER JOIN tmp_buyer_stage ON db.region_code = tmp_buyer_stage.region_code
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4g --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=ads_vova_buyer_gmv_stage_3m" \
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