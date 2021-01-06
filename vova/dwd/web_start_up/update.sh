#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###更新fact_web_start_up
sql="
INSERT OVERWRITE TABLE dwd.dwd_vova_fact_web_start_up PARTITION (pt = '${cur_date}')
SELECT /*+ REPARTITION(1) */
datasource,
domain_userid,
buyer_id,
region_code,
first_page_url,
first_referrer,
min_create_time,
max_create_time
from
(SELECT log.datasource,
       log.domain_userid,
       log.buyer_id,
       log.geo_country                                             AS region_code,
       row_number() OVER (PARTITION BY datasource, buyer_id, domain_userid, geo_country ORDER BY pt DESC, dvce_created_tstamp DESC) AS rank,
       first_value(page_url) OVER (PARTITION BY datasource, buyer_id, domain_userid, geo_country ORDER BY pt, dvce_created_tstamp) AS first_page_url,
       first_value(referrer) OVER (PARTITION BY datasource, buyer_id, domain_userid, geo_country ORDER BY pt, dvce_created_tstamp) AS first_referrer,
       first_value(from_unixtime(cast(dvce_created_tstamp / 1000 AS int))) OVER (PARTITION BY datasource, buyer_id, domain_userid, geo_country ORDER BY pt, dvce_created_tstamp) AS min_create_time,
       from_unixtime(cast(dvce_created_tstamp / 1000 AS int)) AS max_create_time
FROM dwd.dwd_vova_log_page_view log
WHERE log.pt = '${cur_date}'
  AND (log.dp = 'airyclub' OR log.dp = 'vova')
  AND log.platform in ('web', 'pc')) su
WHERE su.rank = 1;
"

#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=dwd_vova_fact_web_start_up" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi


sql="
drop table if exists tmp.tmp_vova_web_first_url_data;
create table tmp.tmp_vova_web_first_url_data as
SELECT domain_userid,
       first_page_url,
       first_referrer,
       CASE
           WHEN utm_medium IS NOT NULL
               THEN lower(utm_medium)
           WHEN a_m IS NOT NULL
               THEN lower(a_m)
           WHEN media_source = 'newsletter'
               THEN lower(media_source)
           WHEN reffer_medium IS NOT NULL
               THEN 'organic'
           WHEN reffer_medium IS NULL AND utm_source IS NULL AND a_s IS NULL AND media_source IS NULL
               THEN 'direct'
           ELSE NULL END AS medium,
       CASE
           WHEN utm_source IS NOT NULL
               THEN lower(utm_source)
           WHEN a_s IS NOT NULL
               THEN lower(a_s)
           WHEN media_source = 'newsletter'
               THEN lower(media_source)
           WHEN reffer_medium IS NOT NULL
               THEN lower(reffer_medium)
           WHEN reffer_medium IS NULL AND utm_medium IS NULL AND a_m IS NULL AND media_source IS NULL
               THEN 'direct'
           ELSE NULL END AS source
FROM (
         SELECT domain_userid,
                first_page_url,
                first_referrer,
                parse_url(first_page_url, 'QUERY', 'utm_medium')   AS utm_medium,
                parse_url(first_page_url, 'QUERY', 'a_m')          AS a_m,
                parse_url(first_page_url, 'QUERY', 'utm_source')   AS utm_source,
                parse_url(first_page_url, 'QUERY', 'a_s')          AS a_s,
                parse_url(first_page_url, 'QUERY', 'media_source') AS media_source,
                CASE
                    WHEN first_referrer LIKE '%google%'
                        THEN 'google'
                    WHEN first_referrer LIKE '%bing%'
                        THEN 'bing'
                    WHEN first_referrer LIKE '%criteo%'
                        THEN 'criteo'
                    WHEN first_referrer LIKE '%facebook%'
                        THEN 'facebook'
                    WHEN first_referrer LIKE '%pinterest%'
                        THEN 'pinterest'
                    WHEN first_referrer LIKE '%admitad%'
                        THEN 'admitad'
                    WHEN first_referrer LIKE '%twitter%'
                        THEN 'twitter'
                    WHEN first_referrer LIKE '%yandex%'
                        THEN 'yandex'
                    WHEN first_referrer LIKE '%youtube%'
                        THEN 'youtube'
                    WHEN first_referrer LIKE '%rtbhouse%'
                        THEN 'rtbhouse'
                    WHEN first_referrer LIKE '%Instagram%'
                        THEN 'Instagram'
                    ELSE NULL END                                  AS reffer_medium
         FROM (
                  SELECT domain_userid,
                         first_page_url,
                         first_referrer
                  FROM (SELECT domain_userid,
                               row_number() OVER (PARTITION BY domain_userid ORDER BY pt, min_create_time) AS rank,
                               first_value(first_page_url)
                                           OVER (PARTITION BY domain_userid ORDER BY pt, min_create_time)  AS first_page_url,
                               first_value(parse_url(first_referrer, 'HOST'))
                                           OVER (PARTITION BY domain_userid ORDER BY pt, min_create_time)  AS first_referrer
                        FROM dwd.dwd_vova_fact_web_start_up su WHERE su.dp = 'airyclub'
                       ) su
                  WHERE su.rank = 1
              ) temp
     ) final
"

#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=tmp_vova_web_first_url_data" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

sql="
-- 获取打点数据点击注册按钮返回成功
INSERT OVERWRITE TABLE tmp.tmp_vova_web_main_process_register PARTITION (pt = '${cur_date}')
SELECT log.datasource,
       log.domain_userid,
       min(dvce_created_tstamp) AS min_create_time
FROM dwd.dwd_vova_log_data log
WHERE log.dp = 'airyclub'
  AND log.platform IN ('web', 'pc')
  AND log.element_name = 'register_success'
  AND log.event_name = 'data'
GROUP BY log.pt, log.datasource, log.domain_userid;

-- 获取domain_userid首次打点信息
INSERT OVERWRITE TABLE dim.dim_vova_web_domain_userid
SELECT t1.domain_userid,
       t2.buyer_id,
       t1.activate_time,
       db.first_order_id,
       db.first_pay_time,
       temp_url.medium AS medium,
       temp_url.source AS source,
       db.reg_time,
       register.register_success_time
FROM (SELECT domain_userid,
             region_code,
             activate_time
      FROM (SELECT domain_userid,
                   region_code,
                   row_number() OVER (PARTITION BY domain_userid ORDER BY pt DESC, min_create_time DESC) AS rank,
                   first_value(min_create_time) OVER (PARTITION BY domain_userid ORDER BY pt, min_create_time)            AS activate_time
            FROM dwd.dwd_vova_fact_web_start_up su WHERE su.dp = 'airyclub'
           ) su
      WHERE su.rank = 1) t1
         LEFT JOIN (
    SELECT domain_userid,
           buyer_id
    FROM (SELECT domain_userid,
                 buyer_id,
                 row_number() OVER (PARTITION BY domain_userid ORDER BY pt, min_create_time) AS rank
          FROM dwd.dwd_vova_fact_web_start_up su
          WHERE su.buyer_id > 0 AND su.dp = 'airyclub') su
    WHERE su.rank = 1
) t2 ON t1.domain_userid = t2.domain_userid
         LEFT JOIN dim.dim_vova_buyers db ON t2.buyer_id = db.buyer_id
         LEFT JOIN tmp.tmp_vova_web_first_url_data temp_url ON temp_url.domain_userid = t1.domain_userid
         left join (
         select reg.domain_userid,
         min(reg.min_create_time) as register_success_time
         from tmp.tmp_vova_web_main_process_register reg
         group by reg.domain_userid
         ) register on register.domain_userid = t1.domain_userid
"

#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=dim_vova_web_domain_userid" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi