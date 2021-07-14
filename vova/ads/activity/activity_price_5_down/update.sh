#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

echo "pre_date: ${pre_date}"
sql="
WITH ads_vova_activity_price_5_down_tmp_gooods AS (
select /*+ REPARTITION(1) */
    cb.goods_id                   goods_id,
    cb.region_id                  region_id,
    'activity-price-5-down-goods' biz_type,
    3                             rp_type,
    cb.ord_cnt / cb.expre_cnt     cr
from dwd.dwd_vova_activity_goods_ctry_behave cb
         inner join dim.dim_vova_goods dg on dg.goods_id = cb.goods_id
where cb.pt = '${pre_date}'
  and cb.is_brand = 0
  and cb.region_id in (0, 3858, 4003, 4017, 4056, 4143, 3974)
  AND dg.shop_price + dg.shipping_fee >= 6
  AND cb.expre_cnt >= 500
  and (
        (
                    dg.second_cat_id in
                    (5902, 5903, 5904, 5905, 5906, 5907, 5911, 5909, 5939, 5940, 5941, 5942, 5943, 5944, 5964, 5965,
                     5966, 5967)
                and cb.clk_cnt / cb.expre_cnt > 0.017
                and cb.gmv / cb.click_uv * cb.clk_cnt / cb.expre_cnt * 10000 >= 30
            )
        or
        (
                    dg.second_cat_id in (5830,5841,5983,5984)
                and cb.clk_cnt / cb.expre_cnt > 0.03
                and cb.gmv / cb.click_uv * cb.clk_cnt / cb.expre_cnt * 10000 >= 60
            )
        or
        (
                    dg.second_cat_id = 5891
                and dg.third_cat_id in (6243,6244,6245,6246,6247,6248,6249)
                and cb.clk_cnt / cb.expre_cnt > 0.03
                and cb.gmv / cb.click_uv * cb.clk_cnt / cb.expre_cnt * 10000 >= 60
            )
        or
        (
                    dg.second_cat_id in (5711,5990,5991,5992,5993,5994)
                and cb.clk_cnt / cb.expre_cnt > 0.017
                and cb.gmv / cb.click_uv * cb.clk_cnt / cb.expre_cnt * 10000 >= 60
            )
        or
        (
                    dg.third_cat_id in (6388,6389,6390,6391,6392,6393,6394,6395,6396,6441,6442,6443,6444,6445,6446,6122,5980,6123,6124,6125,6126,6127,6128,6129,6130,6132,6133,6134,6135,5985,6136,6137,6138,6139,6141,6142,6143,5995,5996,5724,5998,5999,5884,5885,5886,5887,5888,5889,5919,5920,5921,5922,5923,5924,5925,5926,6165,6166,6167,6168,6170,6171,6172,6173,6174,6175,6176,6177,6178,6179,6180,6181,6182,6183,6070,6073,6075,6077,6079,6080,6081,6082)
                and cb.clk_cnt / cb.expre_cnt > 0.03
                and cb.gmv / cb.click_uv * cb.clk_cnt / cb.expre_cnt * 10000 >= 60
            )
        or
        (
                    dg.fourth_cat_id in (6259,6260,6257,6258,6026,6027,6028,6029,6030,6031,6032,6033,6034,6035,6036,6037,6038,6040,6041,6042,6043,6044,6045,6046,6047,6049,6050,6051,6052,6053,5981,6056,6058,6059,6060,6061,6063,6064,6065,6066,6067,6184,6185,6186,6187,6188,6083,6084,6085,6086,6087,6088,6089,6090,6091,6092,6093,6094,6095,6096,6097,6098,6099,6101,6102,6103,6104,6105,6106,6107,6108,6109,6110,6111,6112,6113,6114,6115,6116,6117,6118,6119,6120,6121)
                and cb.clk_cnt / cb.expre_cnt > 0.03
                and cb.gmv / cb.click_uv * cb.clk_cnt / cb.expre_cnt * 10000 >= 60
            )
    )

),
ads_vova_activity_price_5_down_tmp_replace as(
SELECT goods_id,
       region_id,
       biz_type,
       rp_type,
       cr,
       row_number() over ( PARTITION BY region_id, biz_type, rp_type, goods_id ORDER BY cr DESC ) grank
from (SELECT nvl(tmp2.min_price_goods_id, tmp1.goods_id) AS goods_id,
             tmp1.region_id,
             tmp1.biz_type,
             tmp1.rp_type,
             tmp1.cr
            FROM ads_vova_activity_price_5_down_tmp_gooods tmp1
               LEFT JOIN dim.dim_vova_goods dg on tmp1.goods_id = dg.goods_id
               LEFT JOIN (
          SELECT mpg.goods_id,
                 mpg.min_price_goods_id,
                 dg.second_cat_id
          FROM ads.ads_vova_min_price_goods_d mpg
                   LEFT JOIN dim.dim_vova_goods dg ON mpg.min_price_goods_id = dg.goods_id
          WHERE pt = '${pre_date}'
            AND strategy = 'c'
      ) tmp2 ON tmp1.goods_id = tmp2.goods_id
          AND dg.second_cat_id = tmp2.second_cat_id)
)

insert overwrite table ads.ads_vova_activity_price_5_down_goods partition(pt='${pre_date}')
select /*+ REPARTITION(1) */
       t1.goods_id,
       t1.region_id,
       t1.biz_type,
       t1.rp_type,
       dg.first_cat_id,
       nvl(dg.second_cat_id, 0) as second_cat_id,
       row_number()                over ( PARTITION BY t1.region_id, t1.biz_type, t1.rp_type ORDER BY t1.cr DESC ) rank
from ads_vova_activity_price_5_down_tmp_replace t1
         left join dim.dim_vova_goods dg on t1.goods_id = dg.goods_id
where t1.grank = 1
;

insert overwrite table ads.ads_vova_activity_price_5_down partition(pt='${pre_date}')
select /*+ REPARTITION(1) */
    cb.goods_id              goods_id,
    cb.region_id             region_id,
    'price-5-down-goods'     biz_type,
    3                        rp_type,
    cb.first_cat_id          first_cat_id,
    nvl(cb.second_cat_id, 0) second_cat_id,
    row_number()             over(partition by region_id ORDER BY cb.ord_cnt / cb.expre_cnt desc) rank
from dwd.dwd_vova_activity_goods_ctry_behave cb
         inner join dim.dim_vova_goods dg on dg.goods_id = cb.goods_id
where cb.pt = '${pre_date}'
  and cb.is_brand = 0
  and cb.region_id in (0, 3858, 4003, 4017, 4056, 4143, 3974)
  AND dg.shop_price + dg.shipping_fee >= 6
;


"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.app.name=ads_vova_activity_price_5_down" \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_activity_clearance_sale" \
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