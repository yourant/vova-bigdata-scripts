#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

sql="
INSERT overwrite TABLE tmp.tmp_brand PARTITION (pt = '${cur_date}')
SELECT 
       '${cur_date}' as event_date,
       nvl(impression.region_code,'') as region_code,
       nvl(impression.platform,'') as platform,
       nvl(impression.first_cat_name,'') as first_cat_name,
       nvl(impression.is_brand,'N') as is_brand,
       nvl(impression.impression_uv,0) as impression_uv,
       nvl(click.click_uv,0) as click_uv,
       nvl(add_cart.add_cart_uv,0) as add_cart_uv,
       nvl(add_cart.add_cart_success_uv,0) as add_cart_success_uv,
       nvl(product_detail.product_detail_uv,0) as product_detail_uv,
       nvl(order_info.pay_num,0) as pay_num,
       nvl(order_info.pay_user,0) as pay_user,
       nvl(order_info.gmv,0) as gmv,
       nvl(start_up.dau,0) as dau
FROM (
         SELECT nvl(temp.platform, 'all')       AS platform,
                nvl(temp.region_code, 'all')    AS region_code,
                nvl(temp.first_cat_name, 'all') AS first_cat_name,
                nvl(temp.is_brand, 'all')       AS is_brand,
                count(*)       AS impression_uv
         FROM (--step1商品曝光
                  SELECT log.virtual_goods_id,
                         nvl(log.os_type, 'NA')                  AS platform,
                         nvl(log.geo_country, 'NALL')            AS region_code,
                         log.device_id,
                         nvl(dg.first_cat_name, '')              AS first_cat_name,
                         nvl(IF(dg.brand_id > 0, 'Y', 'N'), 'N') AS is_brand

                  FROM dwd.dwd_vova_log_goods_impression log
                           INNER JOIN dim.dim_vova_goods dg ON dg.virtual_goods_id = log.virtual_goods_id
                  WHERE log.pt = '${cur_date}'
                    AND log.platform = 'mob'
              ) temp
         GROUP BY CUBE (temp.first_cat_name, temp.platform, temp.region_code, temp.is_brand)
     ) AS impression
         LEFT JOIN
     (
         SELECT nvl(temp.platform, 'all')       AS platform,
                nvl(temp.region_code, 'all')    AS region_code,
                nvl(temp.first_cat_name, 'all') AS first_cat_name,
                nvl(temp.is_brand, 'all')       AS is_brand,
                count(*)       AS click_uv
         FROM (--step2商品点击
                  SELECT log.virtual_goods_id,
                         nvl(log.os_type, 'NA')                  AS platform,
                         nvl(log.geo_country, 'NALL')            AS region_code,
                         log.device_id,
                         nvl(dg.first_cat_name, '')              AS first_cat_name,
                         nvl(IF(dg.brand_id > 0, 'Y', 'N'), 'N') AS is_brand

                  FROM dwd.dwd_vova_log_goods_click log
                           INNER JOIN dim.dim_vova_goods dg ON dg.virtual_goods_id = log.virtual_goods_id
                  WHERE log.pt = '${cur_date}'
                    AND log.platform = 'mob'
              ) temp
         GROUP BY CUBE (temp.first_cat_name, temp.platform, temp.region_code, temp.is_brand)
     ) AS click ON click.region_code = impression.region_code
         AND click.first_cat_name = impression.first_cat_name
         AND click.platform = impression.platform
         AND click.is_brand = impression.is_brand

         LEFT JOIN
     (
         SELECT nvl(temp.platform, 'all')                    AS platform,
                nvl(temp.region_code, 'all')                 AS region_code,
                nvl(temp.first_cat_name, 'all')              AS first_cat_name,
                nvl(temp.is_brand, 'all')                    AS is_brand,
                count(DISTINCT pdAddToCartSuccess_device_id) AS add_cart_success_uv,
                count(DISTINCT pdAddToCartClick_device_id)   AS add_cart_uv
         FROM (--step3商品加购
                  SELECT CAST(log.element_id AS BIGINT)                                   AS virtual_goods_id,
                         nvl(log.os_type, 'NA')                                           AS platform,
                         nvl(log.geo_country, 'NALL')                                     AS region_code,
                         nvl(dg.first_cat_name, '')                                       AS first_cat_name,
                         nvl(IF(dg.brand_id > 0, 'Y', 'N'), 'N')                          AS is_brand,
                         if(log.element_name = 'pdAddToCartSuccess', log.device_id, NULL) AS pdAddToCartSuccess_device_id,
                         if(log.element_name = 'pdAddToCartClick', log.device_id, NULL)   AS pdAddToCartClick_device_id
                  FROM dwd.dwd_vova_log_common_click log
                           INNER JOIN dim.dim_vova_goods dg ON dg.virtual_goods_id = CAST(log.element_id AS BIGINT)
                  WHERE log.pt = '${cur_date}'
                    AND log.platform = 'mob'
                    AND log.element_name IN ('pdAddToCartSuccess', 'pdAddToCartClick')
                    AND log.element_id IS NOT NULL
              ) temp
         GROUP BY CUBE (temp.first_cat_name, temp.platform, temp.region_code, temp.is_brand)
     ) AS add_cart ON impression.region_code = add_cart.region_code
         AND impression.first_cat_name = add_cart.first_cat_name
         AND impression.platform = add_cart.platform
         AND impression.is_brand = add_cart.is_brand

         LEFT JOIN
     (
         SELECT nvl(temp.platform, 'all')       AS platform,
                nvl(temp.region_code, 'all')    AS region_code,
                nvl(temp.first_cat_name, 'all') AS first_cat_name,
                nvl(temp.is_brand, 'all')       AS is_brand,
                count(DISTINCT device_id)       AS product_detail_uv
         FROM (--step4商详页
                  SELECT log.virtual_goods_id,
                         nvl(log.os_type, 'NA')                  AS platform,
                         nvl(log.geo_country, 'NALL')            AS region_code,
                         log.device_id,
                         nvl(dg.first_cat_name, '')              AS first_cat_name,
                         nvl(IF(dg.brand_id > 0, 'Y', 'N'), 'N') AS is_brand

                  FROM dwd.dwd_vova_log_screen_view log
                           INNER JOIN dim.dim_vova_goods dg ON dg.virtual_goods_id = log.virtual_goods_id
                  WHERE log.pt = '${cur_date}'
                    AND log.platform = 'mob'
                    AND log.page_code = 'product_detail'
              ) temp
         GROUP BY CUBE (temp.first_cat_name, temp.platform, temp.region_code, temp.is_brand)
     )
         AS product_detail ON impression.region_code = product_detail.region_code
         AND impression.first_cat_name = product_detail.first_cat_name
         AND impression.platform = product_detail.platform
         AND impression.is_brand = product_detail.is_brand
         LEFT JOIN
     (
         SELECT nvl(temp.platform, 'all')       AS platform,
                nvl(temp.region_code, 'all')    AS region_code,
                nvl(temp.first_cat_name, 'all') AS first_cat_name,
                nvl(temp.is_brand, 'all')       AS is_brand,
                count(DISTINCT buyer_id)        AS pay_user,
                count(DISTINCT order_id)        AS pay_num,
                sum(gmv)                        AS gmv
         FROM (
                  SELECT nvl(fp.region_code, 'NALL')                       AS region_code,
                         nvl(fp.platform, 'NA')                            AS platform,
                         nvl(g.first_cat_name, '')                         AS first_cat_name,
                         nvl(IF(g.brand_id > 0, 'Y', 'N'), 'N')            AS is_brand,
                         fp.buyer_id,
                         fp.order_id,
                         fp.goods_number * fp.shop_price + fp.shipping_fee AS gmv
                  FROM dwd.dwd_vova_fact_pay fp
                           INNER JOIN dim.dim_vova_goods g ON g.goods_id = fp.goods_id
                  WHERE fp.from_domain LIKE 'api%'
                    AND date(pay_time) = '${cur_date}'
              ) temp
         GROUP BY CUBE (temp.first_cat_name, temp.platform, temp.region_code, temp.is_brand)
     )
         AS order_info ON impression.region_code = order_info.region_code
         AND impression.first_cat_name = order_info.first_cat_name
         AND impression.platform = order_info.platform
         AND impression.is_brand = order_info.is_brand
         LEFT JOIN
     (
         SELECT nvl(platform, 'all')      AS platform,
                nvl(region_code, 'all')   AS region_code,
                count(DISTINCT device_id) AS dau
         FROM dwd.dwd_vova_fact_start_up
         WHERE pt = '${cur_date}' and region_code is not null
         GROUP BY CUBE (platform, region_code)
     ) AS start_up
     ON impression.region_code = start_up.region_code
         AND impression.platform = start_up.platform
"

#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=rpt_brand_stp1" \
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

sql="
INSERT overwrite TABLE dwb.dwb_vova_brand PARTITION (pt = '${cur_date}')
SELECT total.event_date,
       total.region_code,
       total.platform,
       total.first_cat_name,
       nvl(total.impression_uv, 0)         AS total_impression_uv,
       nvl(total.click_uv, 0)              AS total_click_uv,
       nvl(total.add_cart_uv, 0)           AS total_add_cart_uv,
       nvl(total.add_cart_success_uv, 0)   AS total_add_cart_success_uv,
       nvl(total.product_detail_uv, 0)     AS total_product_detail_uv,
       nvl(total.pay_num, 0)               AS total_pay_num,
       nvl(total.pay_user, 0)              AS total_pay_user,
       nvl(total.gmv, 0)                   AS total_gmv,
       nvl(total.dau, 0)                   AS total_dau,
       nvl(brand.impression_uv, 0)           AS ios_impression_uv,
       nvl(brand.click_uv, 0)                AS ios_click_uv,
       nvl(brand.add_cart_uv, 0)             AS ios_add_cart_uv,
       nvl(brand.add_cart_success_uv, 0)     AS ios_add_cart_success_uv,
       nvl(brand.product_detail_uv, 0)       AS ios_product_detail_uv,
       nvl(brand.pay_num, 0)                 AS ios_pay_num,
       nvl(brand.pay_user, 0)                AS ios_pay_user,
       nvl(brand.gmv, 0)                     AS ios_gmv,
       nvl(brand.dau, 0)                     AS ios_dau,
       nvl(not_brand.impression_uv, 0)       AS android_impression_uv,
       nvl(not_brand.click_uv, 0)            AS android_click_uv,
       nvl(not_brand.add_cart_uv, 0)         AS android_add_cart_uv,
       nvl(not_brand.add_cart_success_uv, 0) AS android_add_cart_success_uv,
       nvl(not_brand.product_detail_uv, 0)   AS android_product_detail_uv,
       nvl(not_brand.pay_num, 0)             AS android_pay_num,
       nvl(not_brand.pay_user, 0)            AS android_pay_user,
       nvl(not_brand.gmv, 0)                 AS android_gmv,
       nvl(not_brand.dau, 0)                 AS android_dau
FROM (
         SELECT event_date,
                region_code,
                platform,
                first_cat_name,
                is_brand,
                impression_uv,
                click_uv,
                add_cart_uv,
                add_cart_success_uv,
                product_detail_uv,
                pay_num,
                pay_user,
                gmv,
                dau
         FROM tmp.tmp_brand
         WHERE pt = '${cur_date}'
               AND is_brand = 'all'
     ) total
         LEFT JOIN (
    SELECT event_date,
           region_code,
           platform,
           first_cat_name,
           is_brand,
           impression_uv,
           click_uv,
           add_cart_uv,
           add_cart_success_uv,
           product_detail_uv,
           pay_num,
           pay_user,
           gmv,
           dau
    FROM tmp.tmp_brand
    WHERE pt = '${cur_date}'
      AND is_brand = 'Y'
) brand ON total.event_date = brand.event_date
    AND total.region_code = brand.region_code
    AND total.first_cat_name = brand.first_cat_name
    AND total.platform = brand.platform
         LEFT JOIN (
    SELECT event_date,
           region_code,
           platform,
           first_cat_name,
           is_brand,
           impression_uv,
           click_uv,
           add_cart_uv,
           add_cart_success_uv,
           product_detail_uv,
           pay_num,
           pay_user,
           gmv,
           dau
    FROM tmp.tmp_brand
    WHERE pt = '${cur_date}'
      AND is_brand = 'N'
) not_brand ON total.event_date = not_brand.event_date
    AND total.region_code = not_brand.region_code
    AND total.first_cat_name = not_brand.first_cat_name
    AND total.platform = not_brand.platform
"

#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=rpt_brand_stp2" \
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
if [ $? -ne 0 ];then
  exit 1
fi