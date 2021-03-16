#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
#dependence
#dim_vova_order_goods
#dwd_vova_log_goods_impression
#dwd_vova_log_goods_click
sql="
INSERT OVERWRITE TABLE ads.ads_vova_goods_display_sort PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(5) */
dg.goods_id,
final.gender,
final.platform,
final.project_name,
sum(final.order_cnt) AS sales_order,
sum(final.gmv) AS gmv,
sum(final.impressions) AS impressions,
sum(final.clicks) AS clicks,
sum(final.users) AS users,
current_timestamp() AS last_update_time
from
(
SELECT virtual_goods_id,
       gender,
       platform,
       project_name,
       COUNT(DISTINCT order_id) AS order_cnt,
       SUM(gmv)                 AS gmv,
       0                        AS impressions,
       0                        AS clicks,
       0                        AS users
FROM (
         SELECT dog.virtual_goods_id,
                dog.order_id,
                dog.shop_price * dog.goods_number + dog.shipping_fee                                AS gmv,
                IF(dog.gender = '', 'unknown', gender)                                  AS gender,
                dog.datasource                                                          AS project_name,
                CASE WHEN dog.order_source = 'app' THEN 'mob' ELSE dog.order_source END AS platform
         FROM dim.dim_vova_order_goods dog
         WHERE dog.pay_status >= 1
           AND dog.parent_order_id = 0
           AND date(dog.pay_time) >= date_sub('${cur_date}', 6)
           AND date(dog.pay_time) <= '{cur_date}'
     ) temp
GROUP BY temp.virtual_goods_id, temp.gender, temp.platform, temp.project_name
UNION ALL
SELECT virtual_goods_id,
       gender,
       platform,
       project_name,
       0 AS order_cnt,
       0 AS gmv,
       count(*) AS impressions,
       0        AS clicks,
       0        AS users
FROM (
         SELECT log.virtual_goods_id,
                log.datasource         AS project_name,
                CASE
                    WHEN log.platform = 'mob'
                        THEN 'mob'
                    ELSE 'web' END     AS platform,
                CASE
                    WHEN log.gender = 'female'
                        THEN 'female'
                    WHEN log.gender = 'male'
                        THEN 'male'
                    ELSE 'unknown' END AS gender
         FROM dwd.dwd_vova_log_goods_impression log
           left join dim.dim_zq_site dzs on dzs.datasource = log.datasource
         WHERE log.pt >= date_sub('${cur_date}', 6)
           AND log.pt <= '${cur_date}'
           AND dzs.datasource is null
     ) temp
GROUP BY temp.virtual_goods_id, temp.gender, temp.platform, temp.project_name
UNION ALL
SELECT virtual_goods_id,
       gender,
       platform,
       project_name,
       0 AS order_cnt,
       0 AS gmv,
       0 AS impressions,
       count(*) AS clicks,
       count(DISTINCT device_id) AS users
FROM (
         SELECT log.virtual_goods_id,
                log.device_id,
                log.datasource         AS project_name,
                CASE
                    WHEN log.platform = 'mob'
                        THEN 'mob'
                    ELSE 'web' END     AS platform,
                CASE
                    WHEN log.gender = 'female'
                        THEN 'female'
                    WHEN log.gender = 'male'
                        THEN 'male'
                    ELSE 'unknown' END AS gender
         FROM dwd.dwd_vova_log_goods_click log
           left join dim.dim_zq_site dzs on dzs.datasource = log.datasource
         WHERE log.pt >= date_sub('${cur_date}', 6)
           AND log.pt <= '${cur_date}'
           AND dzs.datasource is null
     ) temp
GROUP BY temp.virtual_goods_id, temp.gender, temp.platform, temp.project_name
) final
inner join dim.dim_vova_goods dg on dg.virtual_goods_id = final.virtual_goods_id
GROUP BY dg.goods_id, final.gender, final.platform, final.project_name
having clicks >0 OR sales_order >0
"

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=20" --conf "spark.app.name=ads_vova_goods_display_sort" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi