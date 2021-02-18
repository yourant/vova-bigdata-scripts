#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

#dependence
#dwd_vova_fact_web_start_up
#dim_vova_web_domain_userid
sql="
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=1000;
INSERT OVERWRITE TABLE dwb.dwb_ac_web_cohort PARTITION (pt)
SELECT /*+ REPARTITION(1) */
       nvl(pt, 'all') AS action_date,
       nvl(datasource, 'all') AS datasource,
       nvl(region_code, 'all') AS region_code,
       nvl(is_activate, 'all') AS is_activate,
       nvl(is_new_user, 'all') AS is_new_user,
       nvl(medium, 'all') AS medium,
       nvl(source, 'all') AS source,
       count(DISTINCT next_0)      AS next_0,
       count(DISTINCT next_1)      AS next_1,
       count(DISTINCT next_3)      AS next_3,
       count(DISTINCT next_7)      AS next_7,
       count(DISTINCT next_28)     AS next_28,
       count(DISTINCT interval_1)  AS interval_1,
       count(DISTINCT interval_3)  AS interval_3,
       count(DISTINCT interval_7)  AS interval_7,
       count(DISTINCT interval_28) AS interval_28,
       nvl(is_new_reg_time, 'all') AS is_new_reg_time,
       nvl(is_new_register_success_time, 'all') AS is_new_register_success_time,
       nvl(pt, 'all') AS pt
FROM (
         SELECT datasource,
                region_code,
                is_activate,
                is_new_user,
                is_new_reg_time,
                is_new_register_success_time,
                medium,
                source,
                pt,
                if(temp.ddiff = 0, temp.domain_userid, NULL)                       AS next_0,
                if(temp.ddiff = 1, temp.domain_userid, NULL)                       AS next_1,
                if(temp.ddiff = 3, temp.domain_userid, NULL)                       AS next_3,
                if(temp.ddiff = 7, temp.domain_userid, NULL)                       AS next_7,
                if(temp.ddiff = 28, temp.domain_userid, NULL)                      AS next_28,
                if(temp.ddiff = 1, temp.domain_userid, NULL)                       AS interval_1,
                if(temp.ddiff >= 1 AND temp.ddiff <= 3, temp.domain_userid, NULL)  AS interval_3,
                if(temp.ddiff >= 1 AND temp.ddiff <= 7, temp.domain_userid, NULL)  AS interval_7,
                if(temp.ddiff >= 1 AND temp.ddiff <= 28, temp.domain_userid, NULL) AS interval_28
         FROM (
                  SELECT nvl(su.region_code, 'NALL')                               AS region_code,
                         su.datasource,
                         nvl(su.domain_userid, NULL)                               AS domain_userid,
                         datediff(su2.pt, su.pt)                                   AS ddiff,
                         if(date(dim_web.activate_time) = su.pt, 'Y', 'N') AS is_activate,
                         if(to_date(dim_web.reg_time) = '${cur_date}' ,'Y','N') AS is_new_reg_time,
                         if(to_date(dim_web.register_success_time) = '${cur_date}' ,'Y','N') AS is_new_register_success_time,
                         if(dim_web.first_order_id IS NULL OR to_date(dim_web.first_pay_time) = su.pt, 'Y','N')                                                   AS is_new_user,
                         nvl(dim_web.medium, 'NA')                                 AS medium,
                         nvl(dim_web.source, 'NA')                                 AS source,
                         su.pt
                  FROM (
                           SELECT domain_userid,
                                  datasource,
                                  region_code,
                                  pt
                           FROM dwd.dwd_vova_fact_web_start_up su
                           WHERE su.pt >= date_sub('${cur_date}', 30)
                             AND su.pt <= '${cur_date}'
                             AND su.datasource in ('airyclub')
                           GROUP BY datasource, domain_userid, region_code, pt) su
                           INNER JOIN (
                      SELECT datasource, domain_userid, pt
                      FROM dwd.dwd_vova_fact_web_start_up su
                      WHERE su.pt >= date_sub('${cur_date}', 30)
                        AND su.pt <= '${cur_date}'
                        AND su.datasource in ('airyclub')
                      GROUP BY domain_userid, pt, datasource
                  ) su2 ON su.domain_userid = su2.domain_userid and su.datasource = su2.datasource
                           LEFT JOIN dim.dim_vova_web_domain_userid dim_web ON su.domain_userid = dim_web.domain_userid AND su.datasource = dim_web.datasource
                  WHERE datediff(su2.pt, su.pt) >= 0
                    AND datediff(su2.pt, su.pt) <= 28
              ) temp
     ) final
      GROUP BY CUBE(final.datasource, final.region_code, final.is_activate, final.is_new_user, final.medium, final.source, final.is_new_reg_time, final.is_new_register_success_time, final.pt)
HAVING action_date != 'all'
"

#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=dwb_ac_web_cohort" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

