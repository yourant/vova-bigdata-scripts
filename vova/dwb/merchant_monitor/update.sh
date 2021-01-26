#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

#rpt_checkout
sql="
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=1000;
INSERT OVERWRITE TABLE dwb.dwb_vova_self_operated_merchant PARTITION (pt)
SELECT /*+ REPARTITION(1) */
       paid_data.pay_date AS action_date,
       paid_data.datasource,
       paid_data.region_code,
       paid_data.platform,
       paid_data.first_cat_name,
       paid_data.goods_id,
       paid_data.gmv,
       paid_data.shipping_fee,
       paid_data.bonus,
       paid_data.purchase_amount,
       paid_data.paid_order_cnt,
       paid_data.paid_buyer_cnt,
       paid_data.cur_sale_goods_cnt,
       paid_data.cur_sale_goods_number_cnt,
       refund_data.refund_amount,
       goods.on_sale_goods_cnt,
       impression_data.impression,
       click_data.click,
       case
       when paid_data.mct_id = 26414 THEN 'Airyclub'
       when paid_data.mct_id = 11630 THEN 'SuperAC'
       when paid_data.mct_id = 36655 THEN 'VogueFD'
       when paid_data.mct_id = 61017 THEN 'dearbuys'
       when paid_data.mct_id = 61028 THEN 'shejoys'
       when paid_data.mct_id = 61235 THEN 'vvshein'
       when paid_data.mct_id = 61310 THEN 'SuperEC'
       else paid_data.mct_id
       end as self_mct_name,
       paid_data.is_brand,
       null confirm_order_cnt,
       paid_data.pay_date AS pt
FROM (
         SELECT nvl(nvl(fp.datasource, 'NA'), 'all')                      AS datasource,
                nvl(nvl(fp.region_code, 'NALL'), 'all')                   AS region_code,
                nvl(nvl(fp.platform, 'NA'), 'all')                        AS platform,
                nvl(nvl(fp.first_cat_name, 'NA'), 'all')                  AS first_cat_name,
                nvl(date(fp.pay_time), 'all')                          AS pay_date,
                nvl(fp.mct_id, 'all')                          AS mct_id,
                nvl(if(dg.brand_id >0 ,'Y', 'N'), 'all')        AS is_brand,
                nvl(fp.goods_id, 'all')                                   AS goods_id,
                sum(fp.shop_price * fp.goods_number + fp.shipping_fee) AS gmv,
                sum(fp.shipping_fee)                                   AS shipping_fee,
                sum(fp.bonus)                                          AS bonus,
                0                                                      AS purchase_amount,
                count(DISTINCT fp.order_goods_id)                      AS paid_order_cnt,
                count(DISTINCT fp.buyer_id)                            AS paid_buyer_cnt,
                count(DISTINCT fp.goods_id)                            AS cur_sale_goods_cnt,
                sum(fp.goods_number)                                   AS cur_sale_goods_number_cnt
         FROM dwd.dwd_vova_fact_pay fp
                  inner join dim.dim_vova_goods dg on dg.goods_id = fp.goods_id
                  -- LEFT JOIN ods_vova_vts.ods_vova_sales_order_goods sog ON sog.order_goods_sn = fp.order_goods_sn
                  -- LEFT JOIN ods.vova_purchase_order_goods pog
                  --          ON pog.purchase_order_goods_id = sog.purchase_order_goods_id
         WHERE date(fp.pay_time) <= '${cur_date}'
           AND date(fp.pay_time) >= date_sub('${cur_date}', 30)
           AND fp.mct_id in (26414, 11630, 36655,61017,61028,61235,61310)
         GROUP BY CUBE (nvl(fp.datasource, 'NA'), nvl(fp.region_code, 'NALL'), nvl(fp.platform, 'NA'), nvl(fp.first_cat_name, 'NA'),
                        fp.goods_id, date(fp.pay_time), fp.mct_id, if(dg.brand_id >0,'Y', 'N'))
     ) paid_data
         LEFT JOIN (
    SELECT nvl(nvl(fp.datasource, 'NA'), 'all')     AS datasource,
           nvl(nvl(fp.region_code, 'NALL'), 'all')  AS region_code,
           nvl(nvl(fp.platform, 'NA'), 'all')       AS platform,
           nvl(nvl(fp.first_cat_name, 'NA'), 'all') AS first_cat_name,
           nvl(date(fp.pay_time), 'all')         AS pay_date,
           nvl(fp.mct_id, 'all')                          AS mct_id,
           nvl(if(dg.brand_id >0 ,'Y', 'N'), 'all')        AS is_brand,
           nvl(fp.goods_id, 'all')                  AS goods_id,
           sum(fr.refund_amount)                    AS refund_amount
    FROM dwd.dwd_vova_fact_pay fp
             inner join dim.dim_vova_goods dg on dg.goods_id = fp.goods_id
             INNER JOIN dwd.dwd_vova_fact_refund fr ON fr.order_goods_id = fp.order_goods_id
    WHERE date(fp.pay_time) <= '${cur_date}'
      AND date(fp.pay_time) >= date_sub('${cur_date}', 30)
      AND fp.mct_id in (26414, 11630, 36655,61017,61028,61235,61310)
    GROUP BY CUBE (nvl(fp.datasource, 'NA'), nvl(fp.region_code, 'NALL'), nvl(fp.platform, 'NA'), fp.goods_id,
                   nvl(fp.first_cat_name, 'NA'),
                   date(fp.pay_time), fp.mct_id, if(dg.brand_id >0 ,'Y', 'N'))
) AS refund_data ON paid_data.datasource = refund_data.datasource
    AND paid_data.region_code = refund_data.region_code
    AND paid_data.platform = refund_data.platform
    AND paid_data.first_cat_name = refund_data.first_cat_name
    AND paid_data.pay_date = refund_data.pay_date
    AND paid_data.goods_id = refund_data.goods_id
    AND paid_data.mct_id = refund_data.mct_id
    AND paid_data.is_brand = refund_data.is_brand
         LEFT JOIN (
    SELECT count(goods_id)            AS on_sale_goods_cnt,
           nvl(first_cat_name, 'all') AS first_cat_name,
           nvl(if(g.brand_id >0 ,'Y', 'N'), 'all')        AS is_brand,
           nvl(g.mct_id, 'all')                          AS mct_id
    FROM dim.dim_vova_goods g
    WHERE g.is_on_sale = 1
      AND g.mct_id in (26414, 11630, 36655,61017,61028,61235,61310)
    GROUP BY CUBE (first_cat_name, g.mct_id, if(g.brand_id >0 ,'Y', 'N'))
) AS goods ON paid_data.first_cat_name = goods.first_cat_name
and paid_data.mct_id = goods.mct_id
    AND paid_data.is_brand = goods.is_brand

left join
(
select
       nvl(nvl(datasource, 'NA'), 'all')     AS datasource,
       nvl(nvl(geo_country, 'NALL'), 'all')  AS region_code,
       nvl(nvl(platform, 'NA'), 'all')  AS platform,
       nvl(nvl(first_cat_name, 'NA'), 'all') AS first_cat_name,
       nvl(if(brand_id >0 ,'Y', 'N'), 'all')        AS is_brand,
       nvl(mct_id, 'all')  AS mct_id,
       nvl(date(pt), 'all')         AS pay_date,
       nvl(goods_id, 'all')                  AS goods_id,
       count(*) as impression
       from

(
SELECT dg.goods_id,
       dg.first_cat_name,
       dg.mct_id,
       dg.brand_id,
       log.datasource,
       log.geo_country,
       log.pt,
        case
           when log.platform = 'pc' then 'pc'
           when log.platform = 'web' then 'mob'
           when log.platform = 'mob' and log.os_type = 'android' then 'android'
           when log.platform = 'mob' and log.os_type = 'ios' then 'ios'
           else ''
           end                                            as platform
FROM dwd.dwd_vova_log_goods_impression log
inner join dim.dim_vova_goods dg on dg.virtual_goods_id = log.virtual_goods_id
WHERE log.pt >= date_sub('${cur_date}', 30)
  AND log.pt <= '${cur_date}'
  AND dg.mct_id in (26414, 11630, 36655,61017,61028,61235,61310) ) temp
GROUP BY CUBE (nvl(datasource, 'NA'), nvl(geo_country, 'NALL'), nvl(platform, 'NA'),goods_id,nvl(first_cat_name, 'NA'),date(pt),mct_id, if(brand_id >0 ,'Y', 'N'))
) impression_data ON paid_data.datasource = impression_data.datasource
    AND paid_data.region_code = impression_data.region_code
    AND paid_data.platform = impression_data.platform
    AND paid_data.first_cat_name = impression_data.first_cat_name
    AND paid_data.pay_date = impression_data.pay_date
    AND paid_data.goods_id = impression_data.goods_id
    AND paid_data.mct_id = impression_data.mct_id
    AND paid_data.is_brand = impression_data.is_brand
left join
(
select
       nvl(nvl(datasource, 'NA'), 'all')     AS datasource,
       nvl(nvl(geo_country, 'NALL'), 'all')  AS region_code,
       nvl(nvl(platform, 'NA'), 'all')  AS platform,
       nvl(mct_id, 'all')  AS mct_id,
       nvl(if(brand_id >0 ,'Y', 'N'), 'all')        AS is_brand,
       nvl(nvl(first_cat_name, 'NA'), 'all') AS first_cat_name,
       nvl(date(pt), 'all')         AS pay_date,
       nvl(goods_id, 'all')                  AS goods_id,
       count(*) as click
       from

(
SELECT dg.goods_id,
       dg.first_cat_name,
       dg.mct_id,
       dg.brand_id,
       log.datasource,
       log.geo_country,
       log.pt,
        case
           when log.platform = 'pc' then 'pc'
           when log.platform = 'web' then 'mob'
           when log.platform = 'mob' and log.os_type = 'android' then 'android'
           when log.platform = 'mob' and log.os_type = 'ios' then 'ios'
           else ''
           end                                            as platform
FROM dwd.dwd_vova_log_goods_click log
inner join dim.dim_vova_goods dg on dg.virtual_goods_id = log.virtual_goods_id
WHERE log.pt >= date_sub('${cur_date}', 30)
  AND log.pt <= '${cur_date}'
  AND dg.mct_id in (26414, 11630, 36655,61017,61028,61235,61310) ) temp
GROUP BY CUBE (nvl(datasource, 'NA'), nvl(geo_country, 'NALL'), nvl(platform, 'NA'),goods_id,nvl(first_cat_name, 'NA'),date(pt),mct_id, if(brand_id >0 ,'Y', 'N'))
) click_data ON paid_data.datasource = click_data.datasource
    AND paid_data.region_code = click_data.region_code
    AND paid_data.platform = click_data.platform
    AND paid_data.first_cat_name = click_data.first_cat_name
    AND paid_data.pay_date = click_data.pay_date
    AND paid_data.goods_id = click_data.goods_id
    AND paid_data.mct_id = click_data.mct_id
    AND paid_data.is_brand = click_data.is_brand
;


INSERT OVERWRITE TABLE dwb.dwb_vova_self_operated_merchant_confirm_order PARTITION (pt)
SELECT
    /*+ REPARTITION(1) */
    date(og.confirm_time)                                     AS action_date,
    nvl(nvl(og.datasource, 'NA'), 'all')                      AS datasource,
    nvl(nvl(og.region_code, 'NALL'), 'all')                   AS region_code,
    nvl(nvl(og.platform, 'NA'), 'all')                        AS platform,
    nvl(nvl(og.first_cat_name, 'NA'), 'all')                  AS first_cat_name,
    case
       when og.mct_id = 26414 THEN 'Airyclub'
       when og.mct_id = 11630 THEN 'SuperAC'
       when og.mct_id = 36655 THEN 'VogueFD'
       when og.mct_id = 61017 THEN 'dearbuys'
       when og.mct_id = 61028 THEN 'shejoys'
       when og.mct_id = 61235 THEN 'vvshein'
       when og.mct_id = 61310 THEN 'SuperEC'
       when og.mct_id is null THEN 'all'
       end as self_mct_name,
    nvl(if(dg.brand_id >0 ,'Y', 'N'), 'all')        AS is_brand,
    count(DISTINCT og.order_goods_id)                      AS confirm_order_cnt,
    nvl(date(og.confirm_time), 'all')                          AS pt
FROM dim.dim_vova_order_goods og
inner join dim.dim_vova_goods dg on dg.goods_id = og.goods_id
WHERE date(og.confirm_time) <= '${cur_date}'
AND date(og.confirm_time) >= date_sub('${cur_date}', 30)
AND og.mct_id in (26414, 11630, 36655,61017,61028,61235,61310)
AND og.confirm_time is not null
GROUP BY CUBE (nvl(og.datasource, 'NA'), nvl(og.region_code, 'NALL'), nvl(og.platform, 'NA'), nvl(og.first_cat_name, 'NA'), date(og.confirm_time), og.mct_id, if(dg.brand_id >0,'Y', 'N'))
"


spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=30" \
--conf "spark.dynamicAllocation.initialExecutors=30" \
--conf "spark.app.name=dwb_vova_self_operated_merchant" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=300000" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi


