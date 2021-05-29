#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
#cur_date2=`date -d "+1 day ${cur_date}" +%Y-%m-%d`
echo "$cur_date"
#echo "$cur_date2"
#UNION ALL
#(
#    SELECT goods_id,
#           first_cat_id,
#           score,
#           top_rank_num,
#           rk,
#           0 AS goods_source_image,
#           1 AS goods_source_basic,
#           0 AS goods_source_text
#    FROM (
#             SELECT r1.goods_id,
#                    dg.first_cat_id,
#                    r1.score,
#                    round(cp.first_cat_gmv / cp.total_gmv, 6) AS                            top_rank_num,
#                    row_number() OVER (PARTITION BY dg.first_cat_id ORDER BY r1.score DESC) rk
#             FROM mlb.mlb_vova_rec_new_goods_base_nb_d r1
#                      INNER JOIN tmp.tmp_vova_ads_new_goods_rec_filter_goods fg on fg.goods_id = r1.goods_id
#                      LEFT JOIN (
#                      select poll_arc.goods_id
#                      from ads.ads_vova_goods_examination_poll poll_arc
#                      where add_test_time >= date_sub('${cur_date}', 61)
#                      ) poll_arc on poll_arc.goods_id = r1.goods_id
#                      INNER JOIN dim.dim_vova_goods dg ON dg.goods_id = r1.goods_id
#                      LEFT JOIN ads.ads_vova_new_goods_merchant_block_list bl on bl.mct_id = dg.mct_id
#                      INNER JOIN tmp.tmp_vova_ads_new_goods_category_percentage cp
#                                 ON cp.first_cat_id = dg.first_cat_id
#             WHERE r1.pt = '${cur_date}'
#               AND dg.brand_id = 0
#               AND dg.is_on_sale = 1
#               AND poll_arc.goods_id is null
#               AND bl.mct_id is null
#         ) t1
#    WHERE t1.rk < 500 * t1.top_rank_num
#)
#msck repair table mlb.mlb_vova_rec_new_goods_base_nb_d;
sql="
msck repair table mlb.mlb_vova_rec_img_relate_group_nb_d;
msck repair table mlb.mlb_vova_rec_new_goods_text_nb_d;
DROP TABLE IF EXISTS tmp.tmp_vova_ads_new_goods_rec_filter_group;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_ads_new_goods_rec_filter_group
SELECT t2.min_price_goods_id,
       t1.group_impressions
FROM (
         SELECT mpg.group_number,
                sum(agp.impressions) AS group_impressions
         FROM (
                  SELECT count(*) AS impressions,
                         log.virtual_goods_id
                  FROM dwd.dwd_vova_log_goods_impression log
                  WHERE log.pt >= date_sub('${cur_date}', 6)
                    AND log.pt <= '${cur_date}'
                    AND log.datasource = 'vova'
                    AND log.platform = 'mob'
                  GROUP BY log.virtual_goods_id
              ) agp
                  INNER JOIN dim.dim_vova_goods dg ON dg.goods_id = agp.virtual_goods_id
                  INNER JOIN ads.ads_vova_min_price_goods_h mpg ON mpg.goods_id = dg.goods_id
         WHERE mpg.pt = '${cur_date}'
           AND mpg.strategy = 'a'
         GROUP BY mpg.group_number
     ) t1
         INNER JOIN
     (
         SELECT mpg.group_number,
                mpg.min_price_goods_id
         FROM ads.ads_vova_min_price_goods_h mpg
         WHERE mpg.pt = '${cur_date}'
           AND mpg.strategy = 'a'
         GROUP BY mpg.group_number, mpg.min_price_goods_id
     ) t2 ON t2.group_number = t1.group_number
;


DROP TABLE IF EXISTS tmp.tmp_vova_ads_new_goods_rec_filter_goods;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_ads_new_goods_rec_filter_goods
SELECT dg.goods_id
FROM (
         SELECT count(*) AS impressions,
                log.virtual_goods_id
         FROM dwd.dwd_vova_log_goods_impression log
         WHERE log.pt >= date_sub('${cur_date}', 6)
           AND log.pt <= '${cur_date}'
           AND log.datasource = 'vova'
           AND log.platform = 'mob'
         GROUP BY log.virtual_goods_id
     ) agp
         INNER JOIN dim.dim_vova_goods dg ON dg.goods_id = agp.virtual_goods_id
         LEFT JOIN ads.ads_vova_min_price_goods_h mpg ON mpg.goods_id = dg.goods_id
WHERE dg.brand_id = 0
  AND dg.is_on_sale = 1
  AND agp.impressions < 300
  AND mpg.goods_id IS NULL

UNION ALL

SELECT dg.goods_id
FROM (
         SELECT count(*) AS impressions,
                log.virtual_goods_id
         FROM dwd.dwd_vova_log_goods_impression log
         WHERE log.pt >= date_sub('${cur_date}', 6)
           AND log.pt <= '${cur_date}'
           AND log.datasource = 'vova'
           AND log.platform = 'mob'
         GROUP BY log.virtual_goods_id
     ) agp
         INNER JOIN dim.dim_vova_goods dg ON dg.goods_id = agp.virtual_goods_id
         INNER JOIN tmp.tmp_vova_ads_new_goods_rec_filter_group fg ON fg.min_price_goods_id = dg.goods_id
WHERE dg.brand_id = 0
  AND dg.is_on_sale = 1
  AND agp.impressions < 300
  AND fg.group_impressions < 500
;

DROP TABLE IF EXISTS tmp.tmp_vova_ads_new_goods_category_percentage;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_ads_new_goods_category_percentage
select
t1.event_date,
t1.first_cat_id,
t1.gmv as first_cat_gmv,
t2.gmv as total_gmv
from
(
select
'${cur_date}' as event_date,
dg.first_cat_id,
sum(fp.goods_number * fp.shop_price + fp.shipping_fee) as gmv
from
dwd.dwd_vova_fact_pay fp
inner join dim.dim_vova_goods dg on dg.goods_id = fp.goods_id
where date(fp.pay_time) > date_sub('${cur_date}', 7)
and date(fp.pay_time) <= '${cur_date}'
group by dg.first_cat_id
) t1
left join
(
select
'${cur_date}' as event_date,
sum(fp.goods_number * fp.shop_price + fp.shipping_fee) as gmv
from
dwd.dwd_vova_fact_pay fp
inner join dim.dim_vova_goods dg on dg.goods_id = fp.goods_id
where date(fp.pay_time) > date_sub('${cur_date}', 7)
and date(fp.pay_time) <= '${cur_date}'
) t2 on t1.event_date = t2.event_date
;


INSERT OVERWRITE TABLE ads.ads_vova_goods_examination_poll_inc PARTITION (pt = '${cur_date}')
SELECT /*+ REPARTITION(1) */
       goods_id,
       sum(goods_source_image)                                          AS goods_source_image,
       sum(goods_source_basic)                                          AS goods_source_basic,
       sum(goods_source_text)                                           AS goods_source_text,
       current_timestamp()                                              AS add_test_time
FROM (
         (
             SELECT goods_id,
                    first_cat_id,
                    score,
                    top_rank_num,
                    rk,
                    1 AS goods_source_image,
                    0 AS goods_source_basic,
                    0 AS goods_source_text
             FROM (
                      SELECT r1.goods_id,
                             dg.first_cat_id,
                             r1.score,
                             round(cp.first_cat_gmv / cp.total_gmv, 6) AS                            top_rank_num,
                             row_number() OVER (PARTITION BY dg.first_cat_id ORDER BY r1.score DESC) rk
                      FROM mlb.mlb_vova_rec_img_relate_group_nb_d r1
                               INNER JOIN tmp.tmp_vova_ads_new_goods_rec_filter_goods fg on fg.goods_id = r1.goods_id
                               LEFT JOIN (
                               select poll_arc.goods_id
                               from ads.ads_vova_goods_examination_poll poll_arc
                               where add_test_time >= date_sub('${cur_date}', 61)
                               ) poll_arc on poll_arc.goods_id = r1.goods_id
                               INNER JOIN dim.dim_vova_goods dg ON dg.goods_id = r1.goods_id
                               LEFT JOIN ads.ads_vova_new_goods_merchant_block_list bl on bl.mct_id = dg.mct_id
                               INNER JOIN tmp.tmp_vova_ads_new_goods_category_percentage cp
                                          ON cp.first_cat_id = dg.first_cat_id
                      WHERE r1.pt = '${cur_date}'
                        AND dg.brand_id = 0
                        AND dg.is_on_sale = 1
                        AND poll_arc.goods_id is null
                        AND bl.mct_id is null
                  ) t1
             WHERE t1.rk < 500 * t1.top_rank_num
         )
         UNION ALL
         (
             SELECT goods_id,
                    first_cat_id,
                    score,
                    top_rank_num,
                    rk,
                    0 AS goods_source_image,
                    0 AS goods_source_basic,
                    1 AS goods_source_text
             FROM (
                      SELECT r1.goods_id,
                             dg.first_cat_id,
                             r1.score,
                             round(cp.first_cat_gmv / cp.total_gmv, 6) AS                            top_rank_num,
                             row_number() OVER (PARTITION BY dg.first_cat_id ORDER BY r1.score DESC) rk
                      FROM mlb.mlb_vova_rec_new_goods_text_nb_d r1
                               INNER JOIN tmp.tmp_vova_ads_new_goods_rec_filter_goods fg on fg.goods_id = r1.goods_id
                               INNER JOIN dim.dim_vova_goods dg ON dg.goods_id = r1.goods_id
                               LEFT JOIN ads.ads_vova_new_goods_merchant_block_list bl on bl.mct_id = dg.mct_id
                               LEFT JOIN (
                               select poll_arc.goods_id
                               from ads.ads_vova_goods_examination_poll poll_arc
                               where add_test_time >= date_sub('${cur_date}', 61)
                               ) poll_arc on poll_arc.goods_id = r1.goods_id
                               INNER JOIN tmp.tmp_vova_ads_new_goods_category_percentage cp
                                          ON cp.first_cat_id = dg.first_cat_id
                      WHERE r1.pt = '${cur_date}'
                        AND dg.brand_id = 0
                        AND dg.is_on_sale = 1
                        AND poll_arc.goods_id is null
                        AND bl.mct_id is null
                  ) t1
             WHERE t1.rk < 500 * t1.top_rank_num
         )
     ) fin
GROUP BY goods_id
;


INSERT OVERWRITE TABLE ads.ads_vova_goods_examination_poll_arc PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
goods_id,
goods_source_image,
goods_source_basic,
goods_source_text,
add_test_time
from
(
select
goods_id,
goods_source_image,
goods_source_basic,
goods_source_text,
add_test_time,
row_number() OVER (PARTITION BY goods_id ORDER BY add_test_time desc) rk
from
(
select
goods_id,
goods_source_image,
goods_source_basic,
goods_source_text,
add_test_time
from
ads.ads_vova_goods_examination_poll_inc
where pt = '${cur_date}'
UNION ALL
select
goods_id,
goods_source_image,
goods_source_basic,
goods_source_text,
add_test_time
from
ads.ads_vova_goods_examination_poll
) t1
) t2
where rk = 1

;
INSERT OVERWRITE TABLE ads.ads_vova_goods_examination_poll
select
/*+ REPARTITION(1) */
goods_id,
goods_source_image,
goods_source_basic,
goods_source_text,
add_test_time
from
ads.ads_vova_goods_examination_poll_arc
where pt = '${cur_date}'
;


"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=20" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=50" \
--conf "spark.app.name=ads_vova_goods_examination_poll" \
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
if [ $? -ne 0 ];then
  exit 1
fi

