#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
sql="
INSERT OVERWRITE TABLE dim.dim_zq_domain_userid
SELECT /*+ REPARTITION(5) */
       t1.datasource,
       t1.domain_userid,
       t1.platform,
       t1.activate_time,
       t1.region_code,
       t1.first_referrer,
       t1.first_page_url,
       t1.buyer_id AS cur_buyer_id,
       t2.buyer_id AS first_buyer_id,
       d2.original_channel
FROM (SELECT datasource,
             domain_userid,
             platform,
             region_code,
             buyer_id,
             activate_time,
             first_referrer,
             first_page_url
      FROM (SELECT datasource,
                   domain_userid,
                   region_code,
                   platform,
                   buyer_id,
                   row_number() OVER (PARTITION BY domain_userid, datasource ORDER BY pt DESC, min_create_time DESC) AS rank,
                   first_value(min_create_time) OVER (PARTITION BY domain_userid, datasource ORDER BY pt, min_create_time) AS activate_time,
                   first_value(first_referrer) OVER (PARTITION BY domain_userid, datasource ORDER BY pt, min_create_time) AS first_referrer,
                   first_value(first_page_url) OVER (PARTITION BY domain_userid, datasource ORDER BY pt, min_create_time) AS first_page_url
            FROM dwd.dwd_zq_fact_web_start_up su
           ) su
      WHERE su.rank = 1) t1
         LEFT JOIN (
    SELECT datasource,
           domain_userid,
           buyer_id
    FROM (SELECT datasource,
                 domain_userid,
                 buyer_id,
                 row_number() OVER (PARTITION BY domain_userid, datasource ORDER BY pt, min_create_time) AS rank
          FROM dwd.dwd_zq_fact_web_start_up su
          WHERE su.buyer_id > 0) su
    WHERE su.rank = 1
) t2 ON t1.domain_userid = t2.domain_userid AND t1.datasource = t2.datasource
inner join dwd.dwd_zq_fact_original_channel d2 on t1.datasource = d2.datasource and t1.domain_userid = d2.domain_userid
;
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.app.name=dim_zq_order_goods" --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.sql.output.merge=true"  --conf "spark.sql.output.coalesceNum=20" -e "$sql"
#如果脚本失败，则报错

if [ $? -ne 0 ];then
  exit 1
fi
