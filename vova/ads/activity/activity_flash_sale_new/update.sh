#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pt=$(date -d "-1 hour" +'%Y-%m-%d-%H')
fi
sql="
WITH tmp_all AS (
SELECT
    cb.goods_id,
    cb.region_id,
    cb.expre_cnt,
    cb.clk_cnt,
    cb.ord_cnt,
    cb.first_cat_id,
    cb.second_cat_id,
    cb.gmv,
    cb.click_uv
FROM
    dwd.dwd_vova_activity_goods_ctry_behave cb
WHERE
    cb.pt = date(date_sub(substr('${pt}',0,10),2))
    AND cb.region_id in (0, 4003, 4056, 4017, 4143, 3858 )
    ),
tmp_red_package_goods(
SELECT
    goods_id,
    1 as type,
    1 as gsn_status
FROM
    ods_vova_vbts.ods_vova_ads_lower_price_goods_red_packet_h
    where is_invalid = 0
    and red_packet_cnt > 0
GROUP BY goods_id

UNION ALL
select
    gcsg.goods_id,
    2 as type,
    min(gca.gsn_status) as gsn_status
from
    ods_vova_vts.ods_vova_gsn_coupon_activity_h gca
inner join ods_vova_vts.ods_vova_gsn_coupon_sign_goods_h gcsg
    on gca.goods_sn = gcsg.goods_sn
where gca.gsn_status in (1,2)
      and gca.is_delete = 0
      and gcsg.remain_num > 0
GROUP BY gcsg.goods_id

),
tmp_ctr_top_list(
select
goods_id,
first_cat_id,
second_cat_id,
type,
ctr
from
(
  select
  tmp_all.goods_id,
  tmp_all.first_cat_id,
  tmp_all.second_cat_id,
  clk_cnt / expre_cnt ctr,
  tmp_red_package_goods.type,
  row_number() over(partition by tmp_red_package_goods.type order by tmp_red_package_goods.gsn_status desc, ord_cnt/expre_cnt desc) rank
  from
  tmp_all
  INNER join tmp_red_package_goods on tmp_all.goods_id = tmp_red_package_goods.goods_id
  where region_id = 0
)
where rank<=400
),
tmp_rand_top_list(
select
goods_id,
first_cat_id,
second_cat_id,
type,
gsn_status,
ctr
from
(
  select
  tmp_all.goods_id,
  clk_cnt / expre_cnt ctr,
  first_cat_id,
  second_cat_id,
  tmp_red_package_goods.type,
  tmp_red_package_goods.gsn_status,
  row_number() over(partition by tmp_red_package_goods.type order by tmp_red_package_goods.gsn_status desc, rand() desc) rank
  from
  tmp_all
  INNER join tmp_red_package_goods on tmp_all.goods_id = tmp_red_package_goods.goods_id
  where expre_cnt < 1000
  and region_id = 0
  and tmp_all.goods_id not in (select goods_id from tmp_ctr_top_list)
)
where rank<=100
),
tmp_top_hot_sell(
select
goods_id,
first_cat_id,
second_cat_id,
type,
region_id,
rank
from
(
  select
  tmp_all.goods_id,
  first_cat_id,
  second_cat_id,
  tmp_all.region_id,
  tmp_red_package_goods.type,
  tmp_red_package_goods.gsn_status,
  row_number() over(partition by tmp_red_package_goods.type,region_id order by tmp_red_package_goods.gsn_status desc, if(type=1, gmv / click_uv * clk_cnt / expre_cnt, ord_cnt/expre_cnt ) desc) rank
  from
  tmp_all
  INNER join tmp_red_package_goods on tmp_all.goods_id = tmp_red_package_goods.goods_id
)
where rank<=30
)



INSERT overwrite TABLE ads.ads_vova_activity_flash_sale_new partition (pt='${pt}')
select
goods_id,
region_id,
biz_type,
rp_type,
first_cat_id,
nvl(second_cat_id,0) second_cat_id,
row_number() over(partition by biz_type order by ctr desc) rank
FROM
(SELECT
    goods_id,
    0 as region_id,
    if(type=1,'on-sale-picked-for-you','up-coming-picked-for-you') as biz_type,
    3 rp_type,
    first_cat_id,
    second_cat_id,
    ctr

FROM
    tmp_ctr_top_list

UNION ALL
SELECT
    goods_id,
    0 as region_id,
    if(type=1,'on-sale-picked-for-you','up-coming-picked-for-you') as biz_type,
    3 rp_type,
    first_cat_id,
    second_cat_id,
    ctr
FROM
    tmp_rand_top_list
)

union all

select
    goods_id,
    region_id,
    if(type=1,'on-sale-hot-selling','up-coming-hot-selling') as biz_type,
    3 as rp_type,
    first_cat_id,
    nvl(second_cat_id,0) second_cat_id,
    rank
from
tmp_top_hot_sell
;

"

echo $sql

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_activity_flash_sale_new" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi