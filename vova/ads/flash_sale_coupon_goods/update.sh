#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

#dependence ods_vova_vts.ods_vova_activity_coupon ;
sql="
insert overwrite table ads.ads_flash_sale_coupon_goods PARTITION (pt = '${cur_date}')
SELECT
/*+ REPARTITION(1) */
    res.goods_id,
    res.first_cat_id,
    res.gmv,
    res.gcr,
    row_number() OVER(PARTITION BY res.first_cat_id ORDER BY res.gmv DESC) gmv_rank,
    row_number() OVER(PARTITION BY res.first_cat_id ORDER BY res.gcr DESC) gcr_rank
FROM (
         SELECT vac.goods_id,
                nvl(dvg.first_cat_id, 0) AS first_cat_id,
                nvl(fin.gmv, 0)          AS gmv,
                nvl(fin.gcr, 0)          AS gcr
         FROM (SELECT DISTINCT goods_id
               FROM ods_vova_vts.ods_vova_activity_coupon vac
               WHERE vac.activity_id = 415334) vac
                  INNER JOIN dim.dim_vova_goods dvg ON dvg.goods_id = vac.goods_id
                  INNER JOIN ods_vova_vbts.ods_vova_ads_lower_price_goods_red_packet gr ON gr.goods_id = vac.goods_id
                  LEFT JOIN
              (
                  SELECT t1.goods_id,
                         sum(impressions)                                                           AS impressions,
                         sum(clicks)                                                                AS clicks,
                         sum(clicks_uv)                                                             AS clicks_uv,
                         sum(gmv)                                                                   AS gmv,
                         nvl(sum(gmv) / sum(clicks_uv) * sum(clicks) / sum(impressions) * 10000, 0) AS gcr
                  FROM (
                           SELECT dg.goods_id,
                                  count(*) AS impressions,
                                  0        AS clicks,
                                  0        AS clicks_uv,
                                  0        AS gmv
                           FROM dwd.dwd_vova_log_goods_impression log
                                    INNER JOIN dim.dim_vova_goods dg ON log.virtual_goods_id = dg.virtual_goods_id
                           WHERE log.pt >= date_sub('${cur_date}', 6)
                             AND log.pt <= '${cur_date}'
                             AND log.datasource IN ('vova')
                             AND log.platform = 'mob'
                           GROUP BY dg.goods_id
                           UNION ALL
                           SELECT dg.goods_id,
                                  0                         AS impressions,
                                  count(*)                  AS clicks,
                                  count(DISTINCT device_id) AS click_uv,
                                  0                         AS gmv
                           FROM dwd.dwd_vova_log_goods_click log
                                    INNER JOIN dim.dim_vova_goods dg ON log.virtual_goods_id = dg.virtual_goods_id
                           WHERE log.pt >= date_sub('${cur_date}', 6)
                             AND log.pt <= '${cur_date}'
                             AND log.datasource IN ('vova')
                             AND log.platform = 'mob'
                           GROUP BY dg.goods_id

                           UNION ALL

                           SELECT fp.goods_id,
                                  0                                                      AS impressions,
                                  0                                                      AS clicks,
                                  0                                                      AS clicks_uv,
                                  sum(fp.goods_number * fp.shop_price + fp.shipping_fee) AS gmv
                           FROM dwd.dwd_vova_fact_pay fp
                           WHERE DATE(fp.pay_time) >= date_sub('${cur_date}', 6)
                             AND DATE(fp.pay_time) <= '${cur_date}'
                             AND fp.datasource = 'vova'
                             AND fp.from_domain LIKE '%api%'
                           GROUP BY fp.goods_id
                       ) t1
                  GROUP BY t1.goods_id
              ) fin ON vac.goods_id = fin.goods_id
         WHERE gr.is_invalid = 0
           AND gr.is_delete = 0
           AND gr.red_packet_cnt > 0
     ) res
;


"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=ads_flash_sale_coupon_goods" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
