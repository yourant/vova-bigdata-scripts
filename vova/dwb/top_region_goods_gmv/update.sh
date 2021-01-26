#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

#TEST
sql="
INSERT overwrite TABLE dwb.dwb_region_top_gmv PARTITION (pt='${cur_date}')
SELECT '${cur_date}'                                                               AS event_date,
       t1.geo_country                                                              AS region_code,
       pay_data.goods_id,
       t2.impression_pv AS impression,
       nvl(t3.click_pv / t2.impression_pv, 0)                                      AS ctr,
       t3.click_uv                                                                 AS users,
       nvl(pay_data.sale_order / t3.click_uv, 0)                                   AS rate,
       pay_data.sale_order                                                         AS orders,
       nvl(t4.add_cart_uv / t2.impression_uv, 0)                                      AS add_cart_rate,
       pay_data.gmv,
       nvl(pay_data.gmv / t3.click_uv * t3.click_pv / t2.impression_pv * 10000, 0) AS gcr
FROM (
         SELECT geo_country
         FROM
         (
         SELECT log.geo_country, count(*) AS impression
         FROM dwd.dwd_vova_log_goods_impression log
         WHERE log.pt = '${cur_date}'
         AND log.geo_country is not null
         GROUP BY log.geo_country
         ORDER BY impression DESC
         LIMIT 20
         ) t1
         UNION
         SELECT 'TW' AS geo_country
     ) t1
         INNER JOIN (
    SELECT t2.gmv,
           t2.goods_id,
           t2.sale_order,
           t2.region_code
    FROM (
             SELECT t1.gmv,
                    t1.goods_id,
                    t1.sale_order,
                    t1.region_code,
                        row_number() OVER (PARTITION BY region_code ORDER BY gmv DESC) AS rank
             FROM (
                      SELECT COUNT(DISTINCT fp.order_goods_id)                      AS sale_order,
                             sum(fp.shop_price * fp.goods_number + fp.shipping_fee) AS gmv,
                             COUNT(DISTINCT fp.buyer_id)                             AS payed_uv,
                             fp.goods_id,
                             fp.region_code
                      FROM dwd.dwd_vova_fact_pay fp
                      WHERE DATE(fp.pay_time) = '${cur_date}'
                      GROUP BY fp.goods_id, fp.region_code
                  ) t1
         ) t2
    WHERE t2.rank <= 100
) AS pay_data ON pay_data.region_code = t1.geo_country
         INNER JOIN dim.dim_vova_goods dg ON dg.goods_id = pay_data.goods_id
         LEFT JOIN
     (
         SELECT log.virtual_goods_id,
                count(*)                      AS impression_pv,
                count(DISTINCT log.device_id) AS impression_uv,
                log.geo_country
         FROM dwd.dwd_vova_log_goods_impression log
         WHERE log.pt = '${cur_date}'
         GROUP BY log.virtual_goods_id, log.geo_country
     ) t2 ON dg.virtual_goods_id = t2.virtual_goods_id
         AND pay_data.region_code = t2.geo_country
         LEFT JOIN
     (
         SELECT log.virtual_goods_id,
                count(*)                      AS click_pv,
                count(DISTINCT log.device_id) AS click_uv,
                log.geo_country
         FROM dwd.dwd_vova_log_goods_click log
         WHERE log.pt = '${cur_date}'
         GROUP BY log.virtual_goods_id, log.geo_country
     ) t3 ON dg.virtual_goods_id = t3.virtual_goods_id
         AND pay_data.region_code = t3.geo_country
         LEFT JOIN
     (
         SELECT cast(element_id AS bigint)   virtual_goods_id,
                log.geo_country,
                count(DISTINCT device_id) AS add_cart_uv
         FROM dwd.dwd_vova_log_common_click log
         WHERE pt = '${cur_date}'
           AND platform = 'mob'
           AND element_name = 'pdAddToCartSuccess'
         GROUP BY cast(element_id AS bigint), log.geo_country
     ) t4 ON dg.virtual_goods_id = t4.virtual_goods_id
         AND pay_data.region_code = t4.geo_country
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=dwb_region_top_gmv" -e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi