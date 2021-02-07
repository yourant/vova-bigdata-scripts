#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

sql="
INSERT OVERWRITE TABLE ads.ads_vova_web_examination_pre PARTITION (pt = '${cur_date}')
SELECT
/*+ REPARTITION(1) */
    final.datasource,
    dg.goods_id,
    nvl(final.impressions, 0)                                                  AS impressions,
    nvl(final.clicks, 0)                                                       AS clicks,
    nvl(final.users, 0)                                                        AS users,
    nvl(final.sales_order, 0)                                                  AS sales_order,
    nvl(final.gmv, 0)                                                          AS gmv,
    nvl(final.add_cart_cnt, 0)                                                 AS add_cart_cnt,
    nvl(final.clicks / final.impressions, 0)                                   AS ctr,
    nvl(final.sales_order / final.users, 0)                                    AS rate,
    nvl(final.gmv / final.users * 100, 0)                                      AS gr,
    nvl(final.gmv / final.users * final.clicks / final.impressions * 10000, 0) AS gcr,
    nvl(final.gmv / final.impressions * 10000, 0)                              AS gmv_cr,
    nvl((final.clicks + final.add_cart_cnt * 3 + final.sales_order * 5) * 100 / final.impressions, 0) AS goods_score
FROM (
         SELECT tmp.virtual_goods_id,
                tmp.datasource,
                sum(impressions)  AS impressions,
                sum(clicks)       AS clicks,
                sum(users)        AS users,
                sum(sales_order)  AS sales_order,
                sum(gmv)          AS gmv,
                sum(add_cart_cnt) AS add_cart_cnt
         FROM (
                  SELECT nvl(virtual_goods_id, 'all')          AS virtual_goods_id,
                         nvl(nvl(log.datasource, 'NA'), 'all') AS datasource,
                         count(*)                              AS impressions,
                         0                                     AS clicks,
                         0                                     AS users,
                         0                                     AS sales_order,
                         0                                     AS gmv,
                         0                                     AS add_cart_cnt
                  FROM dwd.dwd_vova_log_goods_impression log
                  WHERE log.pt >= date_sub('${cur_date}', 29)
                    AND log.pt <= '${cur_date}'
                    AND log.dp IN ('vova', 'airyclub')
                    AND log.datasource IN ('vova', 'airyclub')
                    AND log.virtual_goods_id IS NOT NULL
                  GROUP BY CUBE (nvl(log.datasource, 'NA'), virtual_goods_id)

                  UNION ALL

                  SELECT nvl(virtual_goods_id, 'all')                                               AS virtual_goods_id,
                         nvl(nvl(log.datasource, 'NA'), 'all')                                      AS datasource,
                         0                                                                          AS impressions,
                         count(*)                                                                   AS clicks,
                         count(DISTINCT if(log.platform = 'mob', log.device_id, log.domain_userid)) AS users,
                         0                                                                          AS sales_order,
                         0                                                                          AS gmv,
                         0                                                                          AS add_cart_cnt
                  FROM dwd.dwd_vova_log_goods_click log
                  WHERE log.pt >= date_sub('${cur_date}', 29)
                    AND log.pt <= '${cur_date}'
                    AND log.dp IN ('vova', 'airyclub')
                    AND log.datasource IN ('vova', 'airyclub')
                    AND log.virtual_goods_id IS NOT NULL
                  GROUP BY CUBE (nvl(log.datasource, 'NA'), virtual_goods_id)

                  UNION ALL

                  SELECT nvl(log.element_id, 'all')            AS virtual_goods_id,
                         nvl(nvl(log.datasource, 'NA'), 'all') AS datasource,
                         0                                     AS impressions,
                         0                                     AS clicks,
                         0                                     AS users,
                         0                                     AS sales_order,
                         0                                     AS gmv,
                         count(*)                              AS add_cart_cnt
                  FROM dwd.dwd_vova_log_common_click log
                  WHERE log.pt >= date_sub('${cur_date}', 29)
                    AND log.pt <= '${cur_date}'
                    AND log.dp IN ('vova', 'airyclub')
                    AND log.datasource IN ('vova', 'airyclub')
                    AND log.element_name IN ('pdAddToCartSuccess')
                    AND log.element_id IS NOT NULL
                  GROUP BY CUBE (nvl(log.datasource, 'NA'), virtual_goods_id)

                  UNION ALL

                  SELECT nvl(dg.virtual_goods_id, 'all')                        AS virtual_goods_id,
                         nvl(nvl(fp.datasource, 'NA'), 'all')                   AS datasource,
                         0                                                      AS impressions,
                         0                                                      AS clicks,
                         0                                                      AS users,
                         COUNT(DISTINCT fp.order_id)                            AS sales_order,
                         sum(fp.goods_number * fp.shop_price + fp.shipping_fee) AS gmv,
                         0                                                      AS add_cart_cnt
                  FROM dwd.dwd_vova_fact_pay fp
                           INNER JOIN dim.dim_vova_goods dg ON dg.goods_id = fp.goods_id
                  WHERE DATE(fp.pay_time) >= date_sub('${cur_date}', 29)
                    AND DATE(fp.pay_time) <= '${cur_date}'
                    AND fp.datasource IN ('vova', 'airyclub')
                  GROUP BY CUBE (nvl(fp.datasource, 'NA'), dg.virtual_goods_id)
              ) tmp
         GROUP BY tmp.datasource, tmp.virtual_goods_id
     ) final
         INNER JOIN dim.dim_vova_goods dg ON dg.virtual_goods_id = final.virtual_goods_id
;

INSERT OVERWRITE TABLE ads.ads_vova_web_examination_1w_pre PARTITION (pt = '${cur_date}')
SELECT
/*+ REPARTITION(1) */
    'vova' AS datasource,
    dg.goods_id,
    nvl(final.impressions, 0)                                                  AS impressions,
    nvl(final.clicks, 0)                                                       AS clicks,
    nvl(final.users, 0)                                                        AS users,
    nvl(final.sales_order, 0)                                                  AS sales_order,
    nvl(final.gmv, 0)                                                          AS gmv,
    nvl(final.add_cart_cnt, 0)                                                 AS add_cart_cnt,
    nvl(final.clicks / final.impressions, 0)                                   AS ctr,
    nvl(final.sales_order / final.users, 0)                                    AS rate,
    nvl(final.gmv / final.users * 100, 0)                                      AS gr,
    nvl(final.gmv / final.users * final.clicks / final.impressions * 10000, 0) AS gcr,
    nvl(final.gmv / final.impressions * 10000, 0)                              AS gmv_cr,
    nvl((final.clicks + final.add_cart_cnt * 3 + final.sales_order * 5) * 100 / final.impressions, 0) AS goods_score
FROM (
         SELECT tmp.virtual_goods_id,
                sum(impressions)  AS impressions,
                sum(clicks)       AS clicks,
                sum(users)        AS users,
                sum(sales_order)  AS sales_order,
                sum(gmv)          AS gmv,
                sum(add_cart_cnt) AS add_cart_cnt
         FROM (
                  SELECT log.virtual_goods_id,
                         count(*)                              AS impressions,
                         0                                     AS clicks,
                         0                                     AS users,
                         0                                     AS sales_order,
                         0                                     AS gmv,
                         0                                     AS add_cart_cnt
                  FROM dwd.dwd_vova_log_goods_impression log
                  WHERE log.pt >= date_sub('${cur_date}', 6)
                    AND log.pt <= '${cur_date}'
                    AND log.dp IN ('vova')
                    AND log.datasource IN ('vova')
                    AND log.platform IN ('web', 'pc')
                    AND log.virtual_goods_id IS NOT NULL
                  GROUP BY log.virtual_goods_id

                  UNION ALL

                  SELECT log.virtual_goods_id,
                         0                                                                          AS impressions,
                         count(*)                                                                   AS clicks,
                         count(DISTINCT if(log.platform = 'mob', log.device_id, log.domain_userid)) AS users,
                         0                                                                          AS sales_order,
                         0                                                                          AS gmv,
                         0                                                                          AS add_cart_cnt
                  FROM dwd.dwd_vova_log_goods_click log
                  WHERE log.pt >= date_sub('${cur_date}', 6)
                    AND log.pt <= '${cur_date}'
                    AND log.dp IN ('vova')
                    AND log.datasource IN ('vova')
                    AND log.platform IN ('web', 'pc')
                    AND log.virtual_goods_id IS NOT NULL
                  GROUP BY log.virtual_goods_id

                  UNION ALL

                  SELECT log.element_id                        AS virtual_goods_id,
                         0                                     AS impressions,
                         0                                     AS clicks,
                         0                                     AS users,
                         0                                     AS sales_order,
                         0                                     AS gmv,
                         count(*)                              AS add_cart_cnt
                  FROM dwd.dwd_vova_log_common_click log
                  WHERE log.pt >= date_sub('${cur_date}', 6)
                    AND log.pt <= '${cur_date}'
                    AND log.dp IN ('vova')
                    AND log.datasource IN ('vova')
                    AND log.platform IN ('web', 'pc')
                    AND log.element_name IN ('pdAddToCartSuccess')
                    AND log.element_id IS NOT NULL
                  GROUP BY log.element_id

                  UNION ALL

                  SELECT dg.virtual_goods_id,
                         0                                                      AS impressions,
                         0                                                      AS clicks,
                         0                                                      AS users,
                         COUNT(DISTINCT fp.order_id)                            AS sales_order,
                         sum(fp.goods_number * fp.shop_price + fp.shipping_fee) AS gmv,
                         0                                                      AS add_cart_cnt
                  FROM dwd.dwd_vova_fact_pay fp
                           INNER JOIN dim.dim_vova_goods dg ON dg.goods_id = fp.goods_id
                  WHERE DATE(fp.pay_time) >= date_sub('${cur_date}', 6)
                    AND DATE(fp.pay_time) <= '${cur_date}'
                    AND fp.datasource IN ('vova')
                    AND fp.from_domain NOT LIKE '%api%'
                  GROUP BY dg.virtual_goods_id
              ) tmp
         GROUP BY tmp.virtual_goods_id
     ) final
         INNER JOIN dim.dim_vova_goods dg ON dg.virtual_goods_id = final.virtual_goods_id
;


INSERT OVERWRITE TABLE ads.ads_vova_web_examination_poll_arc PARTITION (pt = '${cur_date}')
SELECT
/*+ REPARTITION(1) */
       'vova' as datasource,
       goods_id,
       add_test_time
FROM (
         SELECT goods_id,
                add_test_time,
                row_number() OVER (PARTITION BY goods_id ORDER BY add_test_time ASC) rk
         FROM (
                  SELECT ep.goods_id,
                         current_timestamp() AS add_test_time
                  FROM ads.ads_vova_web_examination_pre ep
                           INNER JOIN (select max(pt) as max_pt from ads.ads_vova_web_examination_pre ep2) ep2 on ep2.max_pt = ep.pt
                           INNER JOIN dim.dim_vova_goods dg ON dg.goods_id = ep.goods_id
                  WHERE ep.datasource = 'all'
                    AND ep.impressions > 1000
                    AND ep.impressions < 10000
                    AND ep.gcr > 80
                    AND dg.brand_id = 0

                  UNION ALL

                  SELECT ep.goods_id,
                         ep.add_test_time
                  FROM ads.ads_vova_web_examination_poll ep
                           INNER JOIN dim.dim_vova_goods dg ON dg.goods_id = ep.goods_id
                  WHERE dg.brand_id = 0
              ) tmp
     ) fin
WHERE fin.rk = 1
;

INSERT OVERWRITE TABLE ads.ads_vova_web_examination_poll
SELECT
/*+ REPARTITION(1) */
datasource,
goods_id,
add_test_time
from
ads.ads_vova_web_examination_poll_arc
where pt = '${cur_date}'
;
"
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=ads_vova_web_examination_pre" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 280" \
--conf "spark.sql.shuffle.partitions=280" \
--conf "spark.dynamicAllocation.maxExecutors=180" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

