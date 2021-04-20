#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
#dependence
#dwd_vova_log_goods_impression
#dwd_vova_log_goods_click
#dwd_vova_log_common_click
#dwd_vova_fact_pay
#dim_vova_goods
#dim_vova_merchant
#dim_vova_category
#ods_vova_brand
sql="
INSERT OVERWRITE TABLE ads.ads_vova_goods_sn_performance_2m PARTITION (pt = '${cur_date}')
SELECT
/*+ REPARTITION(10) */
       final.goods_sn,
       final.datasource,
       final.platform,
       final.region_code,
       nvl(final.impressions, 0) AS impressions,
       nvl(final.clicks, 0) AS clicks,
       nvl(final.users, 0) AS users,
       nvl(final.sales_order, 0) AS sales_order,
       nvl(final.gmv, 0) AS gmv,
       nvl(final.clicks / final.impressions, 0) as ctr,
       nvl(final.sales_order / final.users, 0) as rate,
       nvl(final.gmv / final.users * 100, 0) as gr,
       nvl(final.gmv / final.users * final.clicks / final.impressions * 10000, 0) as gcr,
       final.last_update_time,
       nvl(goods.first_cat_name, '') first_cat_name,
       nvl(goods.second_cat_name, '') second_cat_name,
       nvl(goods.first_cat_id, 0) as first_cat_id,
       nvl(goods.second_cat_id, 0) as second_cat_id,
       nvl(goods.shop_price_amount, 0) as shop_price_amount,
       nvl(goods.is_on_sale, 0) as is_on_sale,
       nvl(goods.brand_id, 0) as brand_id,
       nvl(goods.brand_name, '') as brand_name,
       nvl(goods.three_cat_id, '/') as third_cat_id,
       nvl(goods.three_cat_name, '/') as third_cat_name,
       nvl(goods.four_cat_id, '/') as fourth_cat_id,
       nvl(goods.four_cat_name, '/') as fourth_cat_name
FROM (
         SELECT fin.goods_sn,
                fin.datasource,
                fin.platform,
                fin.region_code,
                sum(impressions)    AS impressions,
                sum(clicks)         AS clicks,
                sum(users)          AS users,
                sum(sales_order)    AS sales_order,
                sum(gmv)            AS gmv,
                current_timestamp() AS last_update_time
         FROM (
                  SELECT nvl(goods_sn, 'all')    AS goods_sn,
                         nvl(platform, 'all')    AS platform,
                         nvl(region_code, 'all') AS region_code,
                         nvl(datasource, 'all')  AS datasource,
                         count(*)                AS impressions,
                         0                       AS clicks,
                         0                       AS users,
                         0                       AS sales_order,
                         0                       AS gmv
                  FROM (
                           SELECT log.virtual_goods_id,
                                  nvl(dg.goods_sn, 'NA')      AS goods_sn,
                                  case when log.platform = 'mob' then 'mob'
                                       when log.platform in ('pc', 'web') then 'web'
                                       else 'others' end as platform,
                                  nvl(log.geo_country, 'NALL') AS region_code,
                                  nvl(log.datasource, 'NA')        AS datasource
                           FROM dwd.dwd_vova_log_goods_impression log
                                    INNER JOIN dim.dim_vova_goods dg ON log.virtual_goods_id = dg.virtual_goods_id
                                    INNER JOIN ods_vova_vtsf.ods_vova_acg_app a on a.data_domain = log.datasource
                           WHERE log.pt >= date_sub('${cur_date}', 59)
                             AND log.pt <= '${cur_date}'
                             
                       ) temp
                  GROUP BY CUBE (temp.goods_sn, temp.datasource, temp.platform, temp.region_code)
                  UNION ALL
                  SELECT nvl(goods_sn, 'all')      AS goods_sn,
                         nvl(platform, 'all')      AS platform,
                         nvl(region_code, 'all')   AS region_code,
                         nvl(datasource, 'all')    AS datasource,
                         0                         AS impressions,
                         count(*)                  AS clicks,
                         count(DISTINCT device_id) AS users,
                         0                         AS sales_order,
                         0                         AS gmv
                  FROM (
                           SELECT log.virtual_goods_id,
                                  case when log.platform = 'mob' then log.device_id
                                       else log.domain_userid end as device_id,
                                  nvl(dg.goods_sn, 'NA')      AS goods_sn,
                                  case when log.platform = 'mob' then 'mob'
                                       when log.platform in ('pc', 'web') then 'web'
                                       else 'others' end as platform,
                                  nvl(log.geo_country, 'NALL') AS region_code,
                                  nvl(log.datasource, 'NA')        AS datasource
                           FROM dwd.dwd_vova_log_goods_click log
                                    INNER JOIN dim.dim_vova_goods dg ON log.virtual_goods_id = dg.virtual_goods_id
                                    INNER JOIN ods_vova_vtsf.ods_vova_acg_app a on a.data_domain = log.datasource
                           WHERE log.pt >= date_sub('${cur_date}', 59)
                             AND log.pt <= '${cur_date}'
                             
                       ) temp
                  GROUP BY CUBE (temp.goods_sn, temp.datasource, temp.platform, temp.region_code)
                  UNION ALL
                  SELECT nvl(goods_sn, 'all')     AS goods_sn,
                         nvl(platform, 'all')     AS platform,
                         nvl(region_code, 'all')  AS region_code,
                         nvl(datasource, 'all')   AS datasource,
                         0                        AS impressions,
                         0                        AS clicks,
                         0                        AS users,
                         COUNT(DISTINCT order_id) AS sales_order,
                         SUM(gmv)                 AS gmv
                  FROM (
                           SELECT dg.goods_sn,
                                  fp.order_id,
                                  nvl(fp.region_code, 'NALL')                       AS region_code,
                                  fp.goods_number * fp.shop_price + fp.shipping_fee AS gmv,
                                  fp.datasource,
                                  IF(fp.from_domain LIKE '%api%', 'mob', 'web')     AS platform
                           FROM dwd.dwd_vova_fact_pay fp
                             INNER JOIN dim.dim_vova_goods dg ON fp.goods_id = dg.goods_id
                           WHERE DATE(fp.pay_time) >= date_sub('${cur_date}', 59)
                             AND DATE(fp.pay_time) <= '${cur_date}'
                       ) temp
                  GROUP BY CUBE (temp.goods_sn, temp.datasource, temp.platform, temp.region_code)
              ) fin
         GROUP BY fin.goods_sn, fin.datasource, fin.platform, fin.region_code
         HAVING goods_sn != 'all'

         UNION ALL

         SELECT fin.goods_sn,
                'app_group' AS datasource,
                fin.platform,
                fin.region_code,
                sum(impressions)    AS impressions,
                sum(clicks)         AS clicks,
                sum(users)          AS users,
                sum(sales_order)    AS sales_order,
                sum(gmv)            AS gmv,
                current_timestamp() AS last_update_time
         FROM (
                  SELECT nvl(goods_sn, 'all')    AS goods_sn,
                         nvl(platform, 'all')    AS platform,
                         nvl(region_code, 'all') AS region_code,
                         nvl(datasource, 'all')  AS datasource,
                         count(*)                AS impressions,
                         0                       AS clicks,
                         0                       AS users,
                         0                       AS sales_order,
                         0                       AS gmv
                  FROM (
                           SELECT log.virtual_goods_id,
                                  nvl(dg.goods_sn, 'NA')      AS goods_sn,
                                  case when log.platform = 'mob' then 'mob'
                                       when log.platform in ('pc', 'web') then 'web'
                                       else 'others' end as platform,
                                  nvl(log.geo_country, 'NALL') AS region_code,
                                  nvl(log.datasource, 'NA')        AS datasource
                           FROM dwd.dwd_vova_log_goods_impression log
                                    INNER JOIN dim.dim_vova_goods dg ON log.virtual_goods_id = dg.virtual_goods_id
                                    INNER JOIN ods_vova_vtsf.ods_vova_acg_app a on a.data_domain = log.datasource
                           WHERE log.pt >= date_sub('${cur_date}', 59)
                             AND log.pt <= '${cur_date}'
                             AND log.dp = 'others'
                             AND log.datasource not in ('vova', 'ac')
                             
                       ) temp
                  GROUP BY CUBE (temp.goods_sn, temp.datasource, temp.platform, temp.region_code)
                  UNION ALL
                  SELECT nvl(goods_sn, 'all')      AS goods_sn,
                         nvl(platform, 'all')      AS platform,
                         nvl(region_code, 'all')   AS region_code,
                         nvl(datasource, 'all')    AS datasource,
                         0                         AS impressions,
                         count(*)                  AS clicks,
                         count(DISTINCT device_id) AS users,
                         0                         AS sales_order,
                         0                         AS gmv
                  FROM (
                           SELECT log.virtual_goods_id,
                                  case when log.platform = 'mob' then log.device_id
                                       else log.domain_userid end as device_id,
                                  nvl(dg.goods_sn, 'NA')      AS goods_sn,
                                  case when log.platform = 'mob' then 'mob'
                                       when log.platform in ('pc', 'web') then 'web'
                                       else 'others' end as platform,
                                  nvl(log.geo_country, 'NALL') AS region_code,
                                  nvl(log.datasource, 'NA')        AS datasource
                           FROM dwd.dwd_vova_log_goods_click log
                                    INNER JOIN dim.dim_vova_goods dg ON log.virtual_goods_id = dg.virtual_goods_id
                                    INNER JOIN ods_vova_vtsf.ods_vova_acg_app a on a.data_domain = log.datasource
                           WHERE log.pt >= date_sub('${cur_date}', 59)
                             AND log.pt <= '${cur_date}'
                             AND log.dp = 'others'
                             AND log.datasource not in ('vova', 'ac')
                             
                       ) temp
                  GROUP BY CUBE (temp.goods_sn, temp.datasource, temp.platform, temp.region_code)
                  UNION ALL
                  SELECT nvl(goods_sn, 'all')     AS goods_sn,
                         nvl(platform, 'all')     AS platform,
                         nvl(region_code, 'all')  AS region_code,
                         nvl(datasource, 'all')   AS datasource,
                         0                        AS impressions,
                         0                        AS clicks,
                         0                        AS users,
                         COUNT(DISTINCT order_id) AS sales_order,
                         SUM(gmv)                 AS gmv
                  FROM (
                           SELECT dg.goods_sn,
                                  fp.order_id,
                                  nvl(fp.region_code, 'NALL')                       AS region_code,
                                  fp.goods_number * fp.shop_price + fp.shipping_fee AS gmv,
                                  fp.datasource,
                                  IF(fp.from_domain LIKE '%api%', 'mob', 'web')     AS platform
                           FROM dwd.dwd_vova_fact_pay fp
                             INNER JOIN dim.dim_vova_goods dg ON fp.goods_id = dg.goods_id
                           WHERE DATE(fp.pay_time) >= date_sub('${cur_date}', 59)
                             AND DATE(fp.pay_time) <= '${cur_date}'
                             AND fp.datasource not in ('vova', 'airyclub')
                       ) temp
                  GROUP BY CUBE (temp.goods_sn, temp.datasource, temp.platform, temp.region_code)
              ) fin
         GROUP BY fin.goods_sn, fin.datasource, fin.platform, fin.region_code
         HAVING goods_sn != 'all' AND fin.datasource = 'all'

     ) final
     LEFT JOIN
     (
     select
     dg.goods_sn,
     first(dg.first_cat_name) as first_cat_name,
     first(dg.second_cat_name) as second_cat_name,
     first(dg.first_cat_id) as first_cat_id,
     first(dg.second_cat_id) as second_cat_id,
     first(dg.shop_price + dg.shipping_fee) as shop_price_amount,
     first(dg.is_on_sale) as is_on_sale,
     first(dg.brand_id) as brand_id,
     first(b.brand_name) as brand_name,
       first(c.three_cat_id) as three_cat_id,
       first(c.three_cat_name) as three_cat_name,
       first(c.four_cat_id) as four_cat_id,
       first(c.four_cat_name) as four_cat_name
     from
     dim.dim_vova_goods dg
        LEFT JOIN ods_vova_vts.ods_vova_brand b ON b.brand_id = dg.brand_id
        left join dim.dim_vova_category c on dg.cat_id = c.cat_id
     group by dg.goods_sn
     ) goods on goods.goods_sn = final.goods_sn
WHERE (final.clicks > 0 OR final.sales_order > 0)
AND final.region_code in ('all', 'FR', 'DE', 'IT', 'ES', 'GB', 'TW')
"

spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_goods_sn_performance_2m" \
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

