#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

#dependence
#dwd_vova_log_goods_click
#dwd_vova_log_common_click
#dwd_vova_log_screen_view
#dwd_vova_fact_pay
#dim_vova_goods
#dim_vova_order_goods

sql="
INSERT OVERWRITE TABLE dwb.dwb_vova_app_goods_guv PARTITION (pt = '${cur_date}')
SELECT
/*+ REPARTITION(5) */
    '${cur_date}' AS event_date,
    final.region_code,
    final.os_type AS platform,
    REPLACE(final.first_cat_name, '\'', '') AS first_cat_name,
    REPLACE(final.second_cat_name, '\'', '') AS second_cat_name,
    REPLACE(final.third_cat_name, '\'', '') AS third_cat_name,
    final.is_brand,
    nvl(final.impressions_guv, 0)      AS impressions_guv,
    nvl(final.impressions_uv, 0)       AS impressions_uv,
    nvl(final.impressions, 0)          AS impressions,
    nvl(final.click_guv, 0)            AS click_guv,
    nvl(final.pd2cart_guv, 0)          AS pd2cart_guv,
    nvl(final.pd2cart_success_guv, 0)  AS pd2cart_success_guv,
    nvl(final.pd2cart_referrer_guv, 0) AS pd2cart_referrer_guv,
    nvl(final.paid_guv, 0)             AS paid_guv,
    nvl(final.order_guv, 0)            AS order_guv
FROM (
         SELECT fin.region_code,
                fin.os_type,
                fin.first_cat_name,
                fin.second_cat_name,
                fin.third_cat_name,
                fin.is_brand,
                sum(impressions_guv)      AS impressions_guv,
                sum(impressions_uv)       AS impressions_uv,
                sum(impressions)          AS impressions,
                sum(click_guv)            AS click_guv,
                sum(pd2cart_guv)          AS pd2cart_guv,
                sum(pd2cart_success_guv)  AS pd2cart_success_guv,
                sum(pd2cart_referrer_guv) AS pd2cart_referrer_guv,
                sum(paid_guv)             AS paid_guv,
                sum(order_guv)            AS order_guv
         FROM (

                  UNION ALL
                  SELECT nvl(nvl(log.geo_country, 'NALL'), 'all')    AS region_code,
                         nvl(nvl(log.os_type, 'NALL'), 'all')        AS os_type,
                         nvl(nvl(dg.first_cat_name, 'NALL'), 'all')  AS first_cat_name,
                         nvl(nvl(dg.second_cat_name, 'NALL'), 'all') AS second_cat_name,
                         nvl(nvl(dg.third_cat_name, 'NALL'), 'all')  AS third_cat_name,
                         nvl(if(dg.brand_id > 0, 'Y', 'N'), 'all') AS is_brand,
                         0                                           AS impressions_guv,
                         0                                           AS impressions_uv,
                         0                                           AS impressions,
                         count(DISTINCT dg.goods_id, log.device_id)  AS click_guv,
                         0                                           AS pd2cart_guv,
                         0                                           AS pd2cart_success_guv,
                         0                                           AS pd2cart_referrer_guv,
                         0                                           AS paid_guv,
                         0                                           AS order_guv
                  FROM dwd.dwd_vova_log_goods_click log
                           INNER JOIN dim.dim_vova_goods dg ON log.virtual_goods_id = dg.virtual_goods_id
                  WHERE log.pt = '${cur_date}'
                    AND log.datasource = 'vova'
                    AND log.dp = 'vova'
                    AND log.platform = 'mob'
                  GROUP BY CUBE (nvl(log.geo_country, 'NALL'), nvl(log.os_type, 'NALL'), nvl(dg.first_cat_name, 'NALL'),
                                 nvl(dg.second_cat_name, 'NALL'), nvl(dg.third_cat_name, 'NALL'),
                                 if(dg.brand_id > 0, 'Y', 'N'))

                  UNION ALL
                  SELECT nvl(nvl(log.geo_country, 'NALL'), 'all')    AS region_code,
                         nvl(nvl(log.os_type, 'NALL'), 'all')        AS os_type,
                         nvl(nvl(dg.first_cat_name, 'NALL'), 'all')  AS first_cat_name,
                         nvl(nvl(dg.second_cat_name, 'NALL'), 'all') AS second_cat_name,
                         nvl(nvl(dg.third_cat_name, 'NALL'), 'all')  AS third_cat_name,
                         nvl(if(dg.brand_id > 0, 'Y', 'N'), 'all') AS is_brand,
                         0                                           AS impressions_guv,
                         0                                           AS impressions_uv,
                         0                                           AS impressions,
                         0                                           AS click_guv,
                         count(DISTINCT dg.goods_id, log.device_id)  AS pd2cart_guv,
                         0                                           AS pd2cart_success_guv,
                         0                                           AS pd2cart_referrer_guv,
                         0                                           AS paid_guv,
                         0                                           AS order_guv
                  FROM dwd.dwd_vova_log_common_click log
                           INNER JOIN dim.dim_vova_goods dg ON log.element_id = dg.virtual_goods_id
                  WHERE log.pt = '${cur_date}'
                    AND log.datasource = 'vova'
                    AND log.dp = 'vova'
                    AND log.platform = 'mob'
                    AND log.element_name = 'pdAddToCartClick'
                  GROUP BY CUBE (nvl(log.geo_country, 'NALL'), nvl(log.os_type, 'NALL'), nvl(dg.first_cat_name, 'NALL'),
                                 nvl(dg.second_cat_name, 'NALL'), nvl(dg.third_cat_name, 'NALL'),
                                 if(dg.brand_id > 0, 'Y', 'N'))

                  UNION ALL
                  SELECT nvl(nvl(log.geo_country, 'NALL'), 'all')    AS region_code,
                         nvl(nvl(log.os_type, 'NALL'), 'all')        AS os_type,
                         nvl(nvl(dg.first_cat_name, 'NALL'), 'all')  AS first_cat_name,
                         nvl(nvl(dg.second_cat_name, 'NALL'), 'all') AS second_cat_name,
                         nvl(nvl(dg.third_cat_name, 'NALL'), 'all')  AS third_cat_name,
                         nvl(if(dg.brand_id > 0, 'Y', 'N'), 'all') AS is_brand,
                         0                                           AS impressions_guv,
                         0                                           AS impressions_uv,
                         0                                           AS impressions,
                         0                                           AS click_guv,
                         0                                           AS pd2cart_guv,
                         count(DISTINCT dg.goods_id, log.device_id)  AS pd2cart_success_guv,
                         0                                           AS pd2cart_referrer_guv,
                         0                                           AS paid_guv,
                         0                                           AS order_guv
                  FROM dwd.dwd_vova_log_common_click log
                           INNER JOIN dim.dim_vova_goods dg ON log.element_id = dg.virtual_goods_id
                  WHERE log.pt = '${cur_date}'
                    AND log.datasource = 'vova'
                    AND log.dp = 'vova'
                    AND log.platform = 'mob'
                    AND log.element_name = 'pdAddToCartSuccess'
                  GROUP BY CUBE (nvl(log.geo_country, 'NALL'), nvl(log.os_type, 'NALL'), nvl(dg.first_cat_name, 'NALL'),
                                 nvl(dg.second_cat_name, 'NALL'), nvl(dg.third_cat_name, 'NALL'),
                                 if(dg.brand_id > 0, 'Y', 'N'))

                  UNION ALL
                  SELECT nvl(nvl(temp.geo_country, 'NALL'), 'all')   AS region_code,
                         nvl(nvl(temp.os_type, 'NALL'), 'all')       AS os_type,
                         nvl(nvl(dg.first_cat_name, 'NALL'), 'all')  AS first_cat_name,
                         nvl(nvl(dg.second_cat_name, 'NALL'), 'all') AS second_cat_name,
                         nvl(nvl(dg.third_cat_name, 'NALL'), 'all')  AS third_cat_name,
                         nvl(if(dg.brand_id > 0, 'Y', 'N'), 'all')   AS is_brand,
                         0                                           AS impressions_guv,
                         0                                           AS impressions_uv,
                         0                                           AS impressions,
                         0                                           AS click_guv,
                         0                                           AS pd2cart_guv,
                         0                                           AS pd2cart_success_guv,
                         count(DISTINCT dg.goods_id, temp.device_id) AS pd2cart_referrer_guv,
                         0                                           AS paid_guv,
                         0                                           AS order_guv
                  FROM (
                           SELECT regexp_extract(referrer, 'goods_id=([0-9]+)', 1) AS virtual_goods_id,
                                  log.geo_country,
                                  log.os_type,
                                  log.device_id
                           FROM dwd.dwd_vova_log_screen_view log
                           WHERE log.pt = '${cur_date}'
                             AND log.datasource = 'vova'
                             AND log.dp = 'vova'
                             AND log.platform = 'mob'
                             AND page_code = 'cart'
                             AND view_type = 'show'
                             AND referrer LIKE '%product_detail%'
                       ) temp
                           INNER JOIN dim.dim_vova_goods dg ON temp.virtual_goods_id = dg.virtual_goods_id
                  WHERE temp.virtual_goods_id IS NOT NULL
                  GROUP BY CUBE (nvl(temp.geo_country, 'NALL'), nvl(temp.os_type, 'NALL'),
                                 nvl(dg.first_cat_name, 'NALL'), nvl(dg.second_cat_name, 'NALL'),
                                 nvl(dg.third_cat_name, 'NALL'),
                                 if(dg.brand_id > 0, 'Y', 'N'))

                  UNION ALL
                  SELECT nvl(nvl(fp.region_code, 'NALL'), 'all')    AS region_code,
                         nvl(nvl(fp.platform, 'NALL'), 'all')        AS os_type,
                         nvl(nvl(dg.first_cat_name, 'NALL'), 'all')  AS first_cat_name,
                         nvl(nvl(dg.second_cat_name, 'NALL'), 'all') AS second_cat_name,
                         nvl(nvl(dg.third_cat_name, 'NALL'), 'all')  AS third_cat_name,
                         nvl(if(dg.brand_id > 0, 'Y', 'N'), 'all') AS is_brand,
                         0                                           AS impressions_guv,
                         0                                           AS impressions_uv,
                         0                                           AS impressions,
                         0                                           AS click_guv,
                         0                                           AS pd2cart_guv,
                         0                                           AS pd2cart_success_guv,
                         0                                           AS pd2cart_referrer_guv,
                         count(DISTINCT fp.goods_id, fp.buyer_id)    AS paid_guv,
                         0                                           AS order_guv
                  FROM dwd.dwd_vova_fact_pay fp
                           INNER JOIN dim.dim_vova_goods dg ON dg.goods_id = fp.goods_id
                  WHERE date(fp.pay_time) = '${cur_date}'
                    AND fp.from_domain like '%api.vova%'
                  GROUP BY CUBE (nvl(fp.region_code, 'NALL'), nvl(fp.platform, 'NALL'), nvl(dg.first_cat_name, 'NALL'),
                                 nvl(dg.second_cat_name, 'NALL'), nvl(dg.third_cat_name, 'NALL'),
                                 if(dg.brand_id > 0, 'Y', 'N'))

                  UNION ALL
                  SELECT nvl(nvl(dog.region_code, 'NALL'), 'all')    AS region_code,
                         nvl(nvl(dog.platform, 'NALL'), 'all')        AS os_type,
                         nvl(nvl(dg.first_cat_name, 'NALL'), 'all')  AS first_cat_name,
                         nvl(nvl(dg.second_cat_name, 'NALL'), 'all') AS second_cat_name,
                         nvl(nvl(dg.third_cat_name, 'NALL'), 'all')  AS third_cat_name,
                         nvl(if(dg.brand_id > 0, 'Y', 'N'), 'all') AS is_brand,
                         0                                           AS impressions_guv,
                         0                                           AS impressions_uv,
                         0                                           AS impressions,
                         0                                           AS click_guv,
                         0                                           AS pd2cart_guv,
                         0                                           AS pd2cart_success_guv,
                         0                                           AS pd2cart_referrer_guv,
                         0                                           AS paid_guv,
                         count(DISTINCT dog.goods_id, dog.buyer_id)  AS order_guv
                  FROM dim.dim_vova_order_goods dog
                           INNER JOIN dim.dim_vova_goods dg ON dg.goods_id = dog.goods_id
                  WHERE date(dog.order_time) = '${cur_date}'
                    AND dog.from_domain like '%api.vova%'
                    AND dog.parent_order_id = 0
                  GROUP BY CUBE (nvl(dog.region_code, 'NALL'), nvl(dog.platform, 'NALL'), nvl(dg.first_cat_name, 'NALL'),
                                 nvl(dg.second_cat_name, 'NALL'), nvl(dg.third_cat_name, 'NALL'),
                                 if(dg.brand_id > 0, 'Y', 'N'))
              ) fin
         GROUP BY fin.region_code, fin.os_type, fin.first_cat_name, fin.second_cat_name, fin.third_cat_name,
                  fin.is_brand
     ) final
;
"
spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=dwb_vova_app_goods_guv" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 280" \
--conf "spark.sql.shuffle.partitions=280" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
