#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

echo "cur_date: $cur_date"
job_name="dwb_vova_user_cohort_stp_1_req_chenkai_${cur_date}"

###逻辑sql
#pt >= date_sub('${cur_date}', 8) UNION pt = date_sub('${cur_date}', 28)
sql="
insert overwrite table dwb.dwb_vova_user_cohort PARTITION (pt = '${cur_date}')
SELECT
/*+ REPARTITION(1) */
       total.event_date,
       total.datasource,
       total.region_code,
       total.main_channel,
       total.is_new_user,
       total.is_new_activate,
       nvl(total.next_0, 0)       AS total_next_0_num,
       nvl(total.next_1, 0)       AS total_next_1_num,
       nvl(total.next_3, 0)       AS total_next_3_num,
       nvl(total.next_7, 0)       AS total_next_7_num,
       nvl(total.next_28, 0)      AS total_next_28_num,
       nvl(total.interval_1, 0)   AS total_interval_1_num,
       nvl(total.interval_2, 0)   AS total_interval_2_num,
       nvl(total.interval_3, 0)   AS total_interval_3_num,
       nvl(total.interval_4, 0)   AS total_interval_4_num,
       nvl(total.interval_5, 0)   AS total_interval_5_num,
       nvl(total.interval_6, 0)   AS total_interval_6_num,
       nvl(total.interval_7, 0)   AS total_interval_7_num,
       nvl(ios.next_0, 0)         AS ios_next_0_num,
       nvl(ios.next_1, 0)         AS ios_next_1_num,
       nvl(ios.next_3, 0)         AS ios_next_3_num,
       nvl(ios.next_7, 0)         AS ios_next_7_num,
       nvl(ios.next_28, 0)        AS ios_next_28_num,
       nvl(ios.interval_1, 0)     AS ios_interval_1_num,
       nvl(ios.interval_2, 0)     AS ios_interval_2_num,
       nvl(ios.interval_3, 0)     AS ios_interval_3_num,
       nvl(ios.interval_4, 0)     AS ios_interval_4_num,
       nvl(ios.interval_5, 0)     AS ios_interval_5_num,
       nvl(ios.interval_6, 0)     AS ios_interval_6_num,
       nvl(ios.interval_7, 0)     AS ios_interval_7_num,
       nvl(android.next_0, 0)     AS android_next_0_num,
       nvl(android.next_1, 0)     AS android_next_1_num,
       nvl(android.next_3, 0)     AS android_next_3_num,
       nvl(android.next_7, 0)     AS android_next_7_num,
       nvl(android.next_28, 0)    AS android_next_28_num,
       nvl(android.interval_1, 0) AS android_interval_1_num,
       nvl(android.interval_2, 0) AS android_interval_2_num,
       nvl(android.interval_3, 0) AS android_interval_3_num,
       nvl(android.interval_4, 0) AS android_interval_4_num,
       nvl(android.interval_5, 0) AS android_interval_5_num,
       nvl(android.interval_6, 0) AS android_interval_6_num,
       nvl(android.interval_7, 0) AS android_interval_7_num
FROM (SELECT nvl(temp2.region_code, 'all')     AS region_code,
             nvl(temp2.datasource, 'all')      AS datasource,
             nvl(temp2.main_channel, 'all')    AS main_channel,
             nvl(temp2.is_new_user, 'all')     AS is_new_user,
             nvl(temp2.is_new_activate, 'all') AS is_new_activate,
             nvl(temp2.pt, 'all')              AS event_date,
             count(DISTINCT temp2.next_0)      AS next_0,
             count(DISTINCT temp2.next_1)      AS next_1,
             count(DISTINCT temp2.next_3)      AS next_3,
             count(DISTINCT temp2.next_7)      AS next_7,
             count(DISTINCT temp2.next_28)     AS next_28,
             count(DISTINCT temp2.next_1)      AS interval_1,
             count(DISTINCT temp2.interval_2)  AS interval_2,
             count(DISTINCT temp2.interval_3)  AS interval_3,
             count(DISTINCT temp2.interval_4)  AS interval_4,
             count(DISTINCT temp2.interval_5)  AS interval_5,
             count(DISTINCT temp2.interval_6)  AS interval_6,
             count(DISTINCT temp2.interval_7)  AS interval_7
      FROM (
               SELECT temp.region_code,
                      temp.datasource,
                      temp.device_id,
                      if(temp.ddiff = 0, temp.device_id, NULL)                      AS next_0,
                      if(temp.ddiff = 1, temp.device_id, NULL)                      AS next_1,
                      if(temp.ddiff = 3, temp.device_id, NULL)                      AS next_3,
                      if(temp.ddiff = 7, temp.device_id, NULL)                      AS next_7,
                      if(temp.ddiff = 28, temp.device_id, NULL)                     AS next_28,
                      if(temp.ddiff >= 1 AND temp.ddiff <= 2, temp.device_id, NULL) AS interval_2,
                      if(temp.ddiff >= 1 AND temp.ddiff <= 3, temp.device_id, NULL) AS interval_3,
                      if(temp.ddiff >= 1 AND temp.ddiff <= 4, temp.device_id, NULL) AS interval_4,
                      if(temp.ddiff >= 1 AND temp.ddiff <= 5, temp.device_id, NULL) AS interval_5,
                      if(temp.ddiff >= 1 AND temp.ddiff <= 6, temp.device_id, NULL) AS interval_6,
                      if(temp.ddiff >= 1 AND temp.ddiff <= 7, temp.device_id, NULL) AS interval_7,
                      temp.main_channel,
                      temp.is_new_user,
                      temp.is_new_activate,
                      temp.pt
               FROM (SELECT nvl(su.region_code, 'NALL')                             AS region_code,
                            nvl(su.datasource, 'NALL')                              AS datasource,
                            nvl(su.device_id, 'NALL')                               AS device_id,
                            datediff(su2.pt, su.pt)                                 AS ddiff,
                            nvl(dd.main_channel, 'NALL')                            AS main_channel,
                            nvl(IF(dd.first_order_id is null, 'Y', 'N'), 'N') AS is_new_user,
                            nvl(IF(DATE(dd.activate_time) = su.pt, 'Y', 'N'), 'N')  AS is_new_activate,
                            su.pt
                     FROM dwd.dwd_vova_fact_start_up su
                              LEFT JOIN dim.dim_vova_devices dd ON su.device_id = dd.device_id
                              AND su.datasource = dd.datasource
                              INNER JOIN dwd.dwd_vova_fact_start_up su2 ON
                             su.device_id = su2.device_id
                             AND su.datasource = su2.datasource
                     WHERE su.pt >= date_sub('${cur_date}', 8)
                      AND datediff(su2.pt, su.pt) in (0,1,2,3,4,5,6,7,28)
                    ) temp) temp2
      GROUP BY CUBE(temp2.region_code, temp2.datasource, temp2.main_channel,
                    temp2.is_new_user, temp2.is_new_activate, temp2.pt)
      HAVING event_date != 'all') AS total
         LEFT JOIN (
    SELECT nvl(temp2.region_code, 'all')     AS region_code,
           nvl(temp2.datasource, 'all')      AS datasource,
           nvl(temp2.main_channel, 'all')    AS main_channel,
           nvl(temp2.is_new_user, 'all')     AS is_new_user,
           nvl(temp2.is_new_activate, 'all') AS is_new_activate,
           nvl(temp2.pt, 'all')              AS event_date,
           count(DISTINCT temp2.next_0)      AS next_0,
           count(DISTINCT temp2.next_1)      AS next_1,
           count(DISTINCT temp2.next_3)      AS next_3,
           count(DISTINCT temp2.next_7)      AS next_7,
           count(DISTINCT temp2.next_28)     AS next_28,
           count(DISTINCT temp2.next_1)      AS interval_1,
           count(DISTINCT temp2.interval_2)  AS interval_2,
           count(DISTINCT temp2.interval_3)  AS interval_3,
           count(DISTINCT temp2.interval_4)  AS interval_4,
           count(DISTINCT temp2.interval_5)  AS interval_5,
           count(DISTINCT temp2.interval_6)  AS interval_6,
           count(DISTINCT temp2.interval_7)  AS interval_7
    FROM (
             SELECT temp.region_code,
                    temp.datasource,
                    temp.device_id,
                    IF(temp.ddiff = 0, temp.device_id, NULL)                      AS next_0,
                    IF(temp.ddiff = 1, temp.device_id, NULL)                      AS next_1,
                    IF(temp.ddiff = 3, temp.device_id, NULL)                      AS next_3,
                    IF(temp.ddiff = 7, temp.device_id, NULL)                      AS next_7,
                    IF(temp.ddiff = 28, temp.device_id, NULL)                     AS next_28,
                    IF(temp.ddiff >= 1 AND temp.ddiff <= 2, temp.device_id, NULL) AS interval_2,
                    IF(temp.ddiff >= 1 AND temp.ddiff <= 3, temp.device_id, NULL) AS interval_3,
                    IF(temp.ddiff >= 1 AND temp.ddiff <= 4, temp.device_id, NULL) AS interval_4,
                    IF(temp.ddiff >= 1 AND temp.ddiff <= 5, temp.device_id, NULL) AS interval_5,
                    IF(temp.ddiff >= 1 AND temp.ddiff <= 6, temp.device_id, NULL) AS interval_6,
                    IF(temp.ddiff >= 1 AND temp.ddiff <= 7, temp.device_id, NULL) AS interval_7,
                    temp.main_channel,
                    temp.is_new_user,
                    temp.is_new_activate,
                    temp.pt
             FROM (SELECT nvl(su.region_code, 'NALL')                             AS region_code,
                          nvl(su.datasource, 'NALL')                              AS datasource,
                          nvl(su.device_id, 'NALL')                               AS device_id,
                          datediff(su2.pt, su.pt)                                 AS ddiff,
                          nvl(dd.main_channel, 'NALL')                            AS main_channel,
                          nvl(IF(dd.first_order_id is null, 'Y', 'N'), 'N') AS is_new_user,
                          nvl(IF(DATE(dd.activate_time) = su.pt, 'Y', 'N'), 'N')  AS is_new_activate,
                          su.pt
                   FROM dwd.dwd_vova_fact_start_up su
                            LEFT JOIN dim.dim_vova_devices dd ON su.device_id = dd.device_id
                            AND su.datasource = dd.datasource
                            INNER JOIN dwd.dwd_vova_fact_start_up su2 ON
                           su.device_id = su2.device_id
                           AND su.datasource = su2.datasource
                     WHERE su.pt >= date_sub('${cur_date}', 8)
                      AND datediff(su2.pt, su.pt) in (0,1,2,3,4,5,6,7,28)
                     AND su.platform = 'ios'
                  ) temp) temp2
    GROUP BY CUBE(temp2.region_code, temp2.datasource, temp2.main_channel,
                  temp2.is_new_user, temp2.is_new_activate, temp2.pt)
    HAVING event_date != 'all') AS ios
                   ON total.region_code = ios.region_code
                       AND total.datasource = ios.datasource
                       AND total.main_channel = ios.main_channel
                       AND total.is_new_user = ios.is_new_user
                       AND total.is_new_activate = ios.is_new_activate
                       AND total.event_date = ios.event_date
         LEFT JOIN (SELECT nvl(temp2.region_code, 'all')     AS region_code,
                           nvl(temp2.datasource, 'all')      AS datasource,
                           nvl(temp2.main_channel, 'all')    AS main_channel,
                           nvl(temp2.is_new_user, 'all')     AS is_new_user,
                           nvl(temp2.is_new_activate, 'all') AS is_new_activate,
                           nvl(temp2.pt, 'all')              AS event_date,
                           count(DISTINCT temp2.next_0)      AS next_0,
                           count(DISTINCT temp2.next_1)      AS next_1,
                           count(DISTINCT temp2.next_3)      AS next_3,
                           count(DISTINCT temp2.next_7)      AS next_7,
                           count(DISTINCT temp2.next_28)     AS next_28,
                           count(DISTINCT temp2.next_1)      AS interval_1,
                           count(DISTINCT temp2.interval_2)  AS interval_2,
                           count(DISTINCT temp2.interval_3)  AS interval_3,
                           count(DISTINCT temp2.interval_4)  AS interval_4,
                           count(DISTINCT temp2.interval_5)  AS interval_5,
                           count(DISTINCT temp2.interval_6)  AS interval_6,
                           count(DISTINCT temp2.interval_7)  AS interval_7
                    FROM (
                             SELECT temp.region_code,
                                    temp.datasource,
                                    temp.device_id,
                                    IF(temp.ddiff = 0, temp.device_id, NULL)                      AS next_0,
                                    IF(temp.ddiff = 1, temp.device_id, NULL)                      AS next_1,
                                    IF(temp.ddiff = 3, temp.device_id, NULL)                      AS next_3,
                                    IF(temp.ddiff = 7, temp.device_id, NULL)                      AS next_7,
                                    IF(temp.ddiff = 28, temp.device_id, NULL)                     AS next_28,
                                    IF(temp.ddiff >= 1 AND temp.ddiff <= 2, temp.device_id, NULL) AS interval_2,
                                    IF(temp.ddiff >= 1 AND temp.ddiff <= 3, temp.device_id, NULL) AS interval_3,
                                    IF(temp.ddiff >= 1 AND temp.ddiff <= 4, temp.device_id, NULL) AS interval_4,
                                    IF(temp.ddiff >= 1 AND temp.ddiff <= 5, temp.device_id, NULL) AS interval_5,
                                    IF(temp.ddiff >= 1 AND temp.ddiff <= 6, temp.device_id, NULL) AS interval_6,
                                    IF(temp.ddiff >= 1 AND temp.ddiff <= 7, temp.device_id, NULL) AS interval_7,
                                    temp.main_channel,
                                    temp.is_new_user,
                                    temp.is_new_activate,
                                    temp.pt
                             FROM (SELECT nvl(su.region_code, 'NALL')                             AS region_code,
                                          nvl(su.datasource, 'NALL')                              AS datasource,
                                          nvl(su.device_id, 'NALL')                               AS device_id,
                                          datediff(su2.pt, su.pt)                                 AS ddiff,
                                          nvl(dd.main_channel, 'NALL')                            AS main_channel,
                                          nvl(IF(dd.first_order_id is null, 'Y', 'N'), 'N') AS is_new_user,
                                          nvl(IF(DATE(dd.activate_time) = su.pt, 'Y', 'N'), 'N')  AS is_new_activate,
                                          su.pt
                                   FROM dwd.dwd_vova_fact_start_up su
                                            LEFT JOIN dim.dim_vova_devices dd ON su.device_id = dd.device_id
                                            AND su.datasource = dd.datasource
                                            INNER JOIN dwd.dwd_vova_fact_start_up su2 ON
                                           su.device_id = su2.device_id
                                           AND su.datasource = su2.datasource
                     WHERE su.pt >= date_sub('${cur_date}', 8)
                      AND datediff(su2.pt, su.pt) in (0,1,2,3,4,5,6,7,28)
                                     AND su.platform = 'android'
                                  ) temp) temp2
                    GROUP BY CUBE(temp2.region_code, temp2.datasource, temp2.main_channel,
                                  temp2.is_new_user, temp2.is_new_activate, temp2.pt)
                    HAVING event_date != 'all') AS android
                   ON total.region_code = android.region_code
                       AND total.datasource = android.datasource
                       AND total.main_channel = android.main_channel
                       AND total.is_new_user = android.is_new_user
                       AND total.is_new_activate = android.is_new_activate
                       AND total.event_date = android.event_date

UNION ALL
SELECT /*+ REPARTITION(1) */
       total.event_date,
       total.datasource,
       total.region_code,
       total.main_channel,
       total.is_new_user,
       total.is_new_activate,
       nvl(total.next_0, 0)       AS total_next_0_num,
       nvl(total.next_1, 0)       AS total_next_1_num,
       nvl(total.next_3, 0)       AS total_next_3_num,
       nvl(total.next_7, 0)       AS total_next_7_num,
       nvl(total.next_28, 0)      AS total_next_28_num,
       nvl(total.interval_1, 0)   AS total_interval_1_num,
       nvl(total.interval_2, 0)   AS total_interval_2_num,
       nvl(total.interval_3, 0)   AS total_interval_3_num,
       nvl(total.interval_4, 0)   AS total_interval_4_num,
       nvl(total.interval_5, 0)   AS total_interval_5_num,
       nvl(total.interval_6, 0)   AS total_interval_6_num,
       nvl(total.interval_7, 0)   AS total_interval_7_num,
       nvl(ios.next_0, 0)         AS ios_next_0_num,
       nvl(ios.next_1, 0)         AS ios_next_1_num,
       nvl(ios.next_3, 0)         AS ios_next_3_num,
       nvl(ios.next_7, 0)         AS ios_next_7_num,
       nvl(ios.next_28, 0)        AS ios_next_28_num,
       nvl(ios.interval_1, 0)     AS ios_interval_1_num,
       nvl(ios.interval_2, 0)     AS ios_interval_2_num,
       nvl(ios.interval_3, 0)     AS ios_interval_3_num,
       nvl(ios.interval_4, 0)     AS ios_interval_4_num,
       nvl(ios.interval_5, 0)     AS ios_interval_5_num,
       nvl(ios.interval_6, 0)     AS ios_interval_6_num,
       nvl(ios.interval_7, 0)     AS ios_interval_7_num,
       nvl(android.next_0, 0)     AS android_next_0_num,
       nvl(android.next_1, 0)     AS android_next_1_num,
       nvl(android.next_3, 0)     AS android_next_3_num,
       nvl(android.next_7, 0)     AS android_next_7_num,
       nvl(android.next_28, 0)    AS android_next_28_num,
       nvl(android.interval_1, 0) AS android_interval_1_num,
       nvl(android.interval_2, 0) AS android_interval_2_num,
       nvl(android.interval_3, 0) AS android_interval_3_num,
       nvl(android.interval_4, 0) AS android_interval_4_num,
       nvl(android.interval_5, 0) AS android_interval_5_num,
       nvl(android.interval_6, 0) AS android_interval_6_num,
       nvl(android.interval_7, 0) AS android_interval_7_num
FROM (SELECT nvl(temp2.region_code, 'all')     AS region_code,
             nvl(temp2.datasource, 'all')      AS datasource,
             nvl(temp2.main_channel, 'all')    AS main_channel,
             nvl(temp2.is_new_user, 'all')     AS is_new_user,
             nvl(temp2.is_new_activate, 'all') AS is_new_activate,
             nvl(temp2.pt, 'all')              AS event_date,
             count(DISTINCT temp2.next_0)      AS next_0,
             count(DISTINCT temp2.next_1)      AS next_1,
             count(DISTINCT temp2.next_3)      AS next_3,
             count(DISTINCT temp2.next_7)      AS next_7,
             count(DISTINCT temp2.next_28)     AS next_28,
             count(DISTINCT temp2.next_1)      AS interval_1,
             count(DISTINCT temp2.interval_2)  AS interval_2,
             count(DISTINCT temp2.interval_3)  AS interval_3,
             count(DISTINCT temp2.interval_4)  AS interval_4,
             count(DISTINCT temp2.interval_5)  AS interval_5,
             count(DISTINCT temp2.interval_6)  AS interval_6,
             count(DISTINCT temp2.interval_7)  AS interval_7
      FROM (
               SELECT temp.region_code,
                      temp.datasource,
                      temp.device_id,
                      if(temp.ddiff = 0, temp.device_id, NULL)                      AS next_0,
                      if(temp.ddiff = 1, temp.device_id, NULL)                      AS next_1,
                      if(temp.ddiff = 3, temp.device_id, NULL)                      AS next_3,
                      if(temp.ddiff = 7, temp.device_id, NULL)                      AS next_7,
                      if(temp.ddiff = 28, temp.device_id, NULL)                     AS next_28,
                      if(temp.ddiff >= 1 AND temp.ddiff <= 2, temp.device_id, NULL) AS interval_2,
                      if(temp.ddiff >= 1 AND temp.ddiff <= 3, temp.device_id, NULL) AS interval_3,
                      if(temp.ddiff >= 1 AND temp.ddiff <= 4, temp.device_id, NULL) AS interval_4,
                      if(temp.ddiff >= 1 AND temp.ddiff <= 5, temp.device_id, NULL) AS interval_5,
                      if(temp.ddiff >= 1 AND temp.ddiff <= 6, temp.device_id, NULL) AS interval_6,
                      if(temp.ddiff >= 1 AND temp.ddiff <= 7, temp.device_id, NULL) AS interval_7,
                      temp.main_channel,
                      temp.is_new_user,
                      temp.is_new_activate,
                      temp.pt
               FROM (SELECT nvl(su.region_code, 'NALL')                             AS region_code,
                            nvl(su.datasource, 'NALL')                              AS datasource,
                            nvl(su.device_id, 'NALL')                               AS device_id,
                            datediff(su2.pt, su.pt)                                 AS ddiff,
                            nvl(dd.main_channel, 'NALL')                            AS main_channel,
                            nvl(IF(dd.first_order_id is null, 'Y', 'N'), 'N') AS is_new_user,
                            nvl(IF(DATE(dd.activate_time) = su.pt, 'Y', 'N'), 'N')  AS is_new_activate,
                            su.pt
                     FROM dwd.dwd_vova_fact_start_up su
                              LEFT JOIN dim.dim_vova_devices dd ON su.device_id = dd.device_id
                              AND su.datasource = dd.datasource
                              INNER JOIN dwd.dwd_vova_fact_start_up su2 ON
                             su.device_id = su2.device_id
                             AND su.datasource = su2.datasource
                     WHERE su.pt = date_sub('${cur_date}', 28)
                      AND datediff(su2.pt, su.pt) in (0,1,2,3,4,5,6,7,28)
                    ) temp) temp2
      GROUP BY CUBE(temp2.region_code, temp2.datasource, temp2.main_channel,
                    temp2.is_new_user, temp2.is_new_activate, temp2.pt)
      HAVING event_date != 'all') AS total
         LEFT JOIN (
    SELECT nvl(temp2.region_code, 'all')     AS region_code,
           nvl(temp2.datasource, 'all')      AS datasource,
           nvl(temp2.main_channel, 'all')    AS main_channel,
           nvl(temp2.is_new_user, 'all')     AS is_new_user,
           nvl(temp2.is_new_activate, 'all') AS is_new_activate,
           nvl(temp2.pt, 'all')              AS event_date,
           count(DISTINCT temp2.next_0)      AS next_0,
           count(DISTINCT temp2.next_1)      AS next_1,
           count(DISTINCT temp2.next_3)      AS next_3,
           count(DISTINCT temp2.next_7)      AS next_7,
           count(DISTINCT temp2.next_28)     AS next_28,
           count(DISTINCT temp2.next_1)      AS interval_1,
           count(DISTINCT temp2.interval_2)  AS interval_2,
           count(DISTINCT temp2.interval_3)  AS interval_3,
           count(DISTINCT temp2.interval_4)  AS interval_4,
           count(DISTINCT temp2.interval_5)  AS interval_5,
           count(DISTINCT temp2.interval_6)  AS interval_6,
           count(DISTINCT temp2.interval_7)  AS interval_7
    FROM (
             SELECT temp.region_code,
                    temp.datasource,
                    temp.device_id,
                    IF(temp.ddiff = 0, temp.device_id, NULL)                      AS next_0,
                    IF(temp.ddiff = 1, temp.device_id, NULL)                      AS next_1,
                    IF(temp.ddiff = 3, temp.device_id, NULL)                      AS next_3,
                    IF(temp.ddiff = 7, temp.device_id, NULL)                      AS next_7,
                    IF(temp.ddiff = 28, temp.device_id, NULL)                     AS next_28,
                    IF(temp.ddiff >= 1 AND temp.ddiff <= 2, temp.device_id, NULL) AS interval_2,
                    IF(temp.ddiff >= 1 AND temp.ddiff <= 3, temp.device_id, NULL) AS interval_3,
                    IF(temp.ddiff >= 1 AND temp.ddiff <= 4, temp.device_id, NULL) AS interval_4,
                    IF(temp.ddiff >= 1 AND temp.ddiff <= 5, temp.device_id, NULL) AS interval_5,
                    IF(temp.ddiff >= 1 AND temp.ddiff <= 6, temp.device_id, NULL) AS interval_6,
                    IF(temp.ddiff >= 1 AND temp.ddiff <= 7, temp.device_id, NULL) AS interval_7,
                    temp.main_channel,
                    temp.is_new_user,
                    temp.is_new_activate,
                    temp.pt
             FROM (SELECT nvl(su.region_code, 'NALL')                             AS region_code,
                          nvl(su.datasource, 'NALL')                              AS datasource,
                          nvl(su.device_id, 'NALL')                               AS device_id,
                          datediff(su2.pt, su.pt)                                 AS ddiff,
                          nvl(dd.main_channel, 'NALL')                            AS main_channel,
                          nvl(IF(dd.first_order_id is null, 'Y', 'N'), 'N') AS is_new_user,
                          nvl(IF(DATE(dd.activate_time) = su.pt, 'Y', 'N'), 'N')  AS is_new_activate,
                          su.pt
                   FROM dwd.dwd_vova_fact_start_up su
                            LEFT JOIN dim.dim_vova_devices dd ON su.device_id = dd.device_id
                            AND su.datasource = dd.datasource
                            INNER JOIN dwd.dwd_vova_fact_start_up su2 ON
                           su.device_id = su2.device_id
                           AND su.datasource = su2.datasource
                     WHERE su.pt = date_sub('${cur_date}', 28)
                      AND datediff(su2.pt, su.pt) in (0,1,2,3,4,5,6,7,28)
                     AND su.platform = 'ios'
                  ) temp) temp2
    GROUP BY CUBE(temp2.region_code, temp2.datasource, temp2.main_channel,
                  temp2.is_new_user, temp2.is_new_activate, temp2.pt)
    HAVING event_date != 'all') AS ios
                   ON total.region_code = ios.region_code
                       AND total.datasource = ios.datasource
                       AND total.main_channel = ios.main_channel
                       AND total.is_new_user = ios.is_new_user
                       AND total.is_new_activate = ios.is_new_activate
                       AND total.event_date = ios.event_date
         LEFT JOIN (SELECT nvl(temp2.region_code, 'all')     AS region_code,
                           nvl(temp2.datasource, 'all')      AS datasource,
                           nvl(temp2.main_channel, 'all')    AS main_channel,
                           nvl(temp2.is_new_user, 'all')     AS is_new_user,
                           nvl(temp2.is_new_activate, 'all') AS is_new_activate,
                           nvl(temp2.pt, 'all')              AS event_date,
                           count(DISTINCT temp2.next_0)      AS next_0,
                           count(DISTINCT temp2.next_1)      AS next_1,
                           count(DISTINCT temp2.next_3)      AS next_3,
                           count(DISTINCT temp2.next_7)      AS next_7,
                           count(DISTINCT temp2.next_28)     AS next_28,
                           count(DISTINCT temp2.next_1)      AS interval_1,
                           count(DISTINCT temp2.interval_2)  AS interval_2,
                           count(DISTINCT temp2.interval_3)  AS interval_3,
                           count(DISTINCT temp2.interval_4)  AS interval_4,
                           count(DISTINCT temp2.interval_5)  AS interval_5,
                           count(DISTINCT temp2.interval_6)  AS interval_6,
                           count(DISTINCT temp2.interval_7)  AS interval_7
                    FROM (
                             SELECT temp.region_code,
                                    temp.datasource,
                                    temp.device_id,
                                    IF(temp.ddiff = 0, temp.device_id, NULL)                      AS next_0,
                                    IF(temp.ddiff = 1, temp.device_id, NULL)                      AS next_1,
                                    IF(temp.ddiff = 3, temp.device_id, NULL)                      AS next_3,
                                    IF(temp.ddiff = 7, temp.device_id, NULL)                      AS next_7,
                                    IF(temp.ddiff = 28, temp.device_id, NULL)                     AS next_28,
                                    IF(temp.ddiff >= 1 AND temp.ddiff <= 2, temp.device_id, NULL) AS interval_2,
                                    IF(temp.ddiff >= 1 AND temp.ddiff <= 3, temp.device_id, NULL) AS interval_3,
                                    IF(temp.ddiff >= 1 AND temp.ddiff <= 4, temp.device_id, NULL) AS interval_4,
                                    IF(temp.ddiff >= 1 AND temp.ddiff <= 5, temp.device_id, NULL) AS interval_5,
                                    IF(temp.ddiff >= 1 AND temp.ddiff <= 6, temp.device_id, NULL) AS interval_6,
                                    IF(temp.ddiff >= 1 AND temp.ddiff <= 7, temp.device_id, NULL) AS interval_7,
                                    temp.main_channel,
                                    temp.is_new_user,
                                    temp.is_new_activate,
                                    temp.pt
                             FROM (SELECT nvl(su.region_code, 'NALL')                             AS region_code,
                                          nvl(su.datasource, 'NALL')                              AS datasource,
                                          nvl(su.device_id, 'NALL')                               AS device_id,
                                          datediff(su2.pt, su.pt)                                 AS ddiff,
                                          nvl(dd.main_channel, 'NALL')                            AS main_channel,
                                          nvl(IF(dd.first_order_id is null, 'Y', 'N'), 'N') AS is_new_user,
                                          nvl(IF(DATE(dd.activate_time) = su.pt, 'Y', 'N'), 'N')  AS is_new_activate,
                                          su.pt
                                   FROM dwd.dwd_vova_fact_start_up su
                                            LEFT JOIN dim.dim_vova_devices dd ON su.device_id = dd.device_id
                                            AND su.datasource = dd.datasource
                                            INNER JOIN dwd.dwd_vova_fact_start_up su2 ON
                                           su.device_id = su2.device_id
                                           AND su.datasource = su2.datasource
                     WHERE su.pt = date_sub('${cur_date}', 28)
                      AND datediff(su2.pt, su.pt) in (0,1,2,3,4,5,6,7,28)
                                     AND su.platform = 'android'
                                  ) temp) temp2
                    GROUP BY CUBE(temp2.region_code, temp2.datasource, temp2.main_channel,
                                  temp2.is_new_user, temp2.is_new_activate, temp2.pt)
                    HAVING event_date != 'all') AS android
                   ON total.region_code = android.region_code
                       AND total.datasource = android.datasource
                       AND total.main_channel = android.main_channel
                       AND total.is_new_user = android.is_new_user
                       AND total.is_new_activate = android.is_new_activate
                       AND total.event_date = android.event_date
UNION ALL

SELECT
/*+ REPARTITION(1) */
       total.event_date,
       'app-group' AS datasource,
       total.region_code,
       total.main_channel,
       total.is_new_user,
       total.is_new_activate,
       nvl(total.next_0, 0)       AS total_next_0_num,
       nvl(total.next_1, 0)       AS total_next_1_num,
       nvl(total.next_3, 0)       AS total_next_3_num,
       nvl(total.next_7, 0)       AS total_next_7_num,
       nvl(total.next_28, 0)      AS total_next_28_num,
       nvl(total.interval_1, 0)   AS total_interval_1_num,
       nvl(total.interval_2, 0)   AS total_interval_2_num,
       nvl(total.interval_3, 0)   AS total_interval_3_num,
       nvl(total.interval_4, 0)   AS total_interval_4_num,
       nvl(total.interval_5, 0)   AS total_interval_5_num,
       nvl(total.interval_6, 0)   AS total_interval_6_num,
       nvl(total.interval_7, 0)   AS total_interval_7_num,
       nvl(ios.next_0, 0)         AS ios_next_0_num,
       nvl(ios.next_1, 0)         AS ios_next_1_num,
       nvl(ios.next_3, 0)         AS ios_next_3_num,
       nvl(ios.next_7, 0)         AS ios_next_7_num,
       nvl(ios.next_28, 0)        AS ios_next_28_num,
       nvl(ios.interval_1, 0)     AS ios_interval_1_num,
       nvl(ios.interval_2, 0)     AS ios_interval_2_num,
       nvl(ios.interval_3, 0)     AS ios_interval_3_num,
       nvl(ios.interval_4, 0)     AS ios_interval_4_num,
       nvl(ios.interval_5, 0)     AS ios_interval_5_num,
       nvl(ios.interval_6, 0)     AS ios_interval_6_num,
       nvl(ios.interval_7, 0)     AS ios_interval_7_num,
       nvl(android.next_0, 0)     AS android_next_0_num,
       nvl(android.next_1, 0)     AS android_next_1_num,
       nvl(android.next_3, 0)     AS android_next_3_num,
       nvl(android.next_7, 0)     AS android_next_7_num,
       nvl(android.next_28, 0)    AS android_next_28_num,
       nvl(android.interval_1, 0) AS android_interval_1_num,
       nvl(android.interval_2, 0) AS android_interval_2_num,
       nvl(android.interval_3, 0) AS android_interval_3_num,
       nvl(android.interval_4, 0) AS android_interval_4_num,
       nvl(android.interval_5, 0) AS android_interval_5_num,
       nvl(android.interval_6, 0) AS android_interval_6_num,
       nvl(android.interval_7, 0) AS android_interval_7_num
FROM (SELECT nvl(temp2.region_code, 'all')     AS region_code,
             nvl(temp2.main_channel, 'all')    AS main_channel,
             nvl(temp2.is_new_user, 'all')     AS is_new_user,
             nvl(temp2.is_new_activate, 'all') AS is_new_activate,
             nvl(temp2.pt, 'all')              AS event_date,
             count(DISTINCT temp2.next_0)      AS next_0,
             count(DISTINCT temp2.next_1)      AS next_1,
             count(DISTINCT temp2.next_3)      AS next_3,
             count(DISTINCT temp2.next_7)      AS next_7,
             count(DISTINCT temp2.next_28)     AS next_28,
             count(DISTINCT temp2.next_1)      AS interval_1,
             count(DISTINCT temp2.interval_2)  AS interval_2,
             count(DISTINCT temp2.interval_3)  AS interval_3,
             count(DISTINCT temp2.interval_4)  AS interval_4,
             count(DISTINCT temp2.interval_5)  AS interval_5,
             count(DISTINCT temp2.interval_6)  AS interval_6,
             count(DISTINCT temp2.interval_7)  AS interval_7
      FROM (
               SELECT temp.region_code,
                      temp.device_id,
                      if(temp.ddiff = 0, temp.device_id, NULL)                      AS next_0,
                      if(temp.ddiff = 1, temp.device_id, NULL)                      AS next_1,
                      if(temp.ddiff = 3, temp.device_id, NULL)                      AS next_3,
                      if(temp.ddiff = 7, temp.device_id, NULL)                      AS next_7,
                      if(temp.ddiff = 28, temp.device_id, NULL)                     AS next_28,
                      if(temp.ddiff >= 1 AND temp.ddiff <= 2, temp.device_id, NULL) AS interval_2,
                      if(temp.ddiff >= 1 AND temp.ddiff <= 3, temp.device_id, NULL) AS interval_3,
                      if(temp.ddiff >= 1 AND temp.ddiff <= 4, temp.device_id, NULL) AS interval_4,
                      if(temp.ddiff >= 1 AND temp.ddiff <= 5, temp.device_id, NULL) AS interval_5,
                      if(temp.ddiff >= 1 AND temp.ddiff <= 6, temp.device_id, NULL) AS interval_6,
                      if(temp.ddiff >= 1 AND temp.ddiff <= 7, temp.device_id, NULL) AS interval_7,
                      temp.main_channel,
                      temp.is_new_user,
                      temp.is_new_activate,
                      temp.pt
               FROM (SELECT nvl(su.region_code, 'NALL')                             AS region_code,
                            nvl(su.datasource, 'NALL')                              AS datasource,
                            nvl(su.device_id, 'NALL')                               AS device_id,
                            datediff(su2.pt, su.pt)                                 AS ddiff,
                            nvl(dd.main_channel, 'NALL')                            AS main_channel,
                            nvl(IF(dd.first_order_id is null, 'Y', 'N'), 'N') AS is_new_user,
                            nvl(IF(DATE(dd.activate_time) = su.pt, 'Y', 'N'), 'N')  AS is_new_activate,
                            su.pt
                     FROM dwd.dwd_vova_fact_start_up su
                              LEFT JOIN dim.dim_vova_devices dd ON su.device_id = dd.device_id
                              AND su.datasource = dd.datasource
                              INNER JOIN dwd.dwd_vova_fact_start_up su2 ON
                             su.device_id = su2.device_id
                             AND su.datasource = su2.datasource
                     WHERE su.pt >= date_sub('${cur_date}', 8)
                      AND datediff(su2.pt, su.pt) in (0,1,2,3,4,5,6,7,28)
                      AND su.datasource not in ('vova', 'airyclub')
                    ) temp) temp2
      GROUP BY CUBE(temp2.region_code, temp2.main_channel,
                    temp2.is_new_user, temp2.is_new_activate, temp2.pt)
      HAVING event_date != 'all') AS total
         LEFT JOIN (
    SELECT nvl(temp2.region_code, 'all')     AS region_code,
           nvl(temp2.main_channel, 'all')    AS main_channel,
           nvl(temp2.is_new_user, 'all')     AS is_new_user,
           nvl(temp2.is_new_activate, 'all') AS is_new_activate,
           nvl(temp2.pt, 'all')              AS event_date,
           count(DISTINCT temp2.next_0)      AS next_0,
           count(DISTINCT temp2.next_1)      AS next_1,
           count(DISTINCT temp2.next_3)      AS next_3,
           count(DISTINCT temp2.next_7)      AS next_7,
           count(DISTINCT temp2.next_28)     AS next_28,
           count(DISTINCT temp2.next_1)      AS interval_1,
           count(DISTINCT temp2.interval_2)  AS interval_2,
           count(DISTINCT temp2.interval_3)  AS interval_3,
           count(DISTINCT temp2.interval_4)  AS interval_4,
           count(DISTINCT temp2.interval_5)  AS interval_5,
           count(DISTINCT temp2.interval_6)  AS interval_6,
           count(DISTINCT temp2.interval_7)  AS interval_7
    FROM (
             SELECT temp.region_code,
                    temp.datasource,
                    temp.device_id,
                    IF(temp.ddiff = 0, temp.device_id, NULL)                      AS next_0,
                    IF(temp.ddiff = 1, temp.device_id, NULL)                      AS next_1,
                    IF(temp.ddiff = 3, temp.device_id, NULL)                      AS next_3,
                    IF(temp.ddiff = 7, temp.device_id, NULL)                      AS next_7,
                    IF(temp.ddiff = 28, temp.device_id, NULL)                     AS next_28,
                    IF(temp.ddiff >= 1 AND temp.ddiff <= 2, temp.device_id, NULL) AS interval_2,
                    IF(temp.ddiff >= 1 AND temp.ddiff <= 3, temp.device_id, NULL) AS interval_3,
                    IF(temp.ddiff >= 1 AND temp.ddiff <= 4, temp.device_id, NULL) AS interval_4,
                    IF(temp.ddiff >= 1 AND temp.ddiff <= 5, temp.device_id, NULL) AS interval_5,
                    IF(temp.ddiff >= 1 AND temp.ddiff <= 6, temp.device_id, NULL) AS interval_6,
                    IF(temp.ddiff >= 1 AND temp.ddiff <= 7, temp.device_id, NULL) AS interval_7,
                    temp.main_channel,
                    temp.is_new_user,
                    temp.is_new_activate,
                    temp.pt
             FROM (SELECT nvl(su.region_code, 'NALL')                             AS region_code,
                          nvl(su.datasource, 'NALL')                              AS datasource,
                          nvl(su.device_id, 'NALL')                               AS device_id,
                          datediff(su2.pt, su.pt)                                 AS ddiff,
                          nvl(dd.main_channel, 'NALL')                            AS main_channel,
                          nvl(IF(dd.first_order_id is null, 'Y', 'N'), 'N') AS is_new_user,
                          nvl(IF(DATE(dd.activate_time) = su.pt, 'Y', 'N'), 'N')  AS is_new_activate,
                          su.pt
                   FROM dwd.dwd_vova_fact_start_up su
                            LEFT JOIN dim.dim_vova_devices dd ON su.device_id = dd.device_id
                            AND su.datasource = dd.datasource
                            INNER JOIN dwd.dwd_vova_fact_start_up su2 ON
                           su.device_id = su2.device_id
                           AND su.datasource = su2.datasource
                     WHERE su.pt >= date_sub('${cur_date}', 8)
                      AND datediff(su2.pt, su.pt) in (0,1,2,3,4,5,6,7,28)
                      AND su.platform = 'ios'
                      AND su.datasource not in ('vova', 'airyclub')
                  ) temp) temp2
    GROUP BY CUBE(temp2.region_code, temp2.main_channel,
                  temp2.is_new_user, temp2.is_new_activate, temp2.pt)
    HAVING event_date != 'all') AS ios
                   ON total.region_code = ios.region_code
                       AND total.main_channel = ios.main_channel
                       AND total.is_new_user = ios.is_new_user
                       AND total.is_new_activate = ios.is_new_activate
                       AND total.event_date = ios.event_date
         LEFT JOIN (SELECT nvl(temp2.region_code, 'all')     AS region_code,
                           nvl(temp2.main_channel, 'all')    AS main_channel,
                           nvl(temp2.is_new_user, 'all')     AS is_new_user,
                           nvl(temp2.is_new_activate, 'all') AS is_new_activate,
                           nvl(temp2.pt, 'all')              AS event_date,
                           count(DISTINCT temp2.next_0)      AS next_0,
                           count(DISTINCT temp2.next_1)      AS next_1,
                           count(DISTINCT temp2.next_3)      AS next_3,
                           count(DISTINCT temp2.next_7)      AS next_7,
                           count(DISTINCT temp2.next_28)     AS next_28,
                           count(DISTINCT temp2.next_1)      AS interval_1,
                           count(DISTINCT temp2.interval_2)  AS interval_2,
                           count(DISTINCT temp2.interval_3)  AS interval_3,
                           count(DISTINCT temp2.interval_4)  AS interval_4,
                           count(DISTINCT temp2.interval_5)  AS interval_5,
                           count(DISTINCT temp2.interval_6)  AS interval_6,
                           count(DISTINCT temp2.interval_7)  AS interval_7
                    FROM (
                             SELECT temp.region_code,
                                    temp.datasource,
                                    temp.device_id,
                                    IF(temp.ddiff = 0, temp.device_id, NULL)                      AS next_0,
                                    IF(temp.ddiff = 1, temp.device_id, NULL)                      AS next_1,
                                    IF(temp.ddiff = 3, temp.device_id, NULL)                      AS next_3,
                                    IF(temp.ddiff = 7, temp.device_id, NULL)                      AS next_7,
                                    IF(temp.ddiff = 28, temp.device_id, NULL)                     AS next_28,
                                    IF(temp.ddiff >= 1 AND temp.ddiff <= 2, temp.device_id, NULL) AS interval_2,
                                    IF(temp.ddiff >= 1 AND temp.ddiff <= 3, temp.device_id, NULL) AS interval_3,
                                    IF(temp.ddiff >= 1 AND temp.ddiff <= 4, temp.device_id, NULL) AS interval_4,
                                    IF(temp.ddiff >= 1 AND temp.ddiff <= 5, temp.device_id, NULL) AS interval_5,
                                    IF(temp.ddiff >= 1 AND temp.ddiff <= 6, temp.device_id, NULL) AS interval_6,
                                    IF(temp.ddiff >= 1 AND temp.ddiff <= 7, temp.device_id, NULL) AS interval_7,
                                    temp.main_channel,
                                    temp.is_new_user,
                                    temp.is_new_activate,
                                    temp.pt
                             FROM (SELECT nvl(su.region_code, 'NALL')                             AS region_code,
                                          nvl(su.datasource, 'NALL')                              AS datasource,
                                          nvl(su.device_id, 'NALL')                               AS device_id,
                                          datediff(su2.pt, su.pt)                                 AS ddiff,
                                          nvl(dd.main_channel, 'NALL')                            AS main_channel,
                                          nvl(IF(dd.first_order_id is null, 'Y', 'N'), 'N') AS is_new_user,
                                          nvl(IF(DATE(dd.activate_time) = su.pt, 'Y', 'N'), 'N')  AS is_new_activate,
                                          su.pt
                                   FROM dwd.dwd_vova_fact_start_up su
                                            LEFT JOIN dim.dim_vova_devices dd ON su.device_id = dd.device_id
                                            AND su.datasource = dd.datasource
                                            INNER JOIN dwd.dwd_vova_fact_start_up su2 ON
                                           su.device_id = su2.device_id
                                           AND su.datasource = su2.datasource
                     WHERE su.pt >= date_sub('${cur_date}', 8)
                      AND datediff(su2.pt, su.pt) in (0,1,2,3,4,5,6,7,28)
                      AND su.platform = 'android'
                      AND su.datasource not in ('vova', 'airyclub')
                                  ) temp) temp2
                    GROUP BY CUBE(temp2.region_code, temp2.main_channel,
                                  temp2.is_new_user, temp2.is_new_activate, temp2.pt)
                    HAVING event_date != 'all') AS android
                   ON total.region_code = android.region_code
                       AND total.main_channel = android.main_channel
                       AND total.is_new_user = android.is_new_user
                       AND total.is_new_activate = android.is_new_activate
                       AND total.event_date = android.event_date

UNION ALL
SELECT /*+ REPARTITION(1) */
       total.event_date,
       'app-group' AS datasource,
       total.region_code,
       total.main_channel,
       total.is_new_user,
       total.is_new_activate,
       nvl(total.next_0, 0)       AS total_next_0_num,
       nvl(total.next_1, 0)       AS total_next_1_num,
       nvl(total.next_3, 0)       AS total_next_3_num,
       nvl(total.next_7, 0)       AS total_next_7_num,
       nvl(total.next_28, 0)      AS total_next_28_num,
       nvl(total.interval_1, 0)   AS total_interval_1_num,
       nvl(total.interval_2, 0)   AS total_interval_2_num,
       nvl(total.interval_3, 0)   AS total_interval_3_num,
       nvl(total.interval_4, 0)   AS total_interval_4_num,
       nvl(total.interval_5, 0)   AS total_interval_5_num,
       nvl(total.interval_6, 0)   AS total_interval_6_num,
       nvl(total.interval_7, 0)   AS total_interval_7_num,
       nvl(ios.next_0, 0)         AS ios_next_0_num,
       nvl(ios.next_1, 0)         AS ios_next_1_num,
       nvl(ios.next_3, 0)         AS ios_next_3_num,
       nvl(ios.next_7, 0)         AS ios_next_7_num,
       nvl(ios.next_28, 0)        AS ios_next_28_num,
       nvl(ios.interval_1, 0)     AS ios_interval_1_num,
       nvl(ios.interval_2, 0)     AS ios_interval_2_num,
       nvl(ios.interval_3, 0)     AS ios_interval_3_num,
       nvl(ios.interval_4, 0)     AS ios_interval_4_num,
       nvl(ios.interval_5, 0)     AS ios_interval_5_num,
       nvl(ios.interval_6, 0)     AS ios_interval_6_num,
       nvl(ios.interval_7, 0)     AS ios_interval_7_num,
       nvl(android.next_0, 0)     AS android_next_0_num,
       nvl(android.next_1, 0)     AS android_next_1_num,
       nvl(android.next_3, 0)     AS android_next_3_num,
       nvl(android.next_7, 0)     AS android_next_7_num,
       nvl(android.next_28, 0)    AS android_next_28_num,
       nvl(android.interval_1, 0) AS android_interval_1_num,
       nvl(android.interval_2, 0) AS android_interval_2_num,
       nvl(android.interval_3, 0) AS android_interval_3_num,
       nvl(android.interval_4, 0) AS android_interval_4_num,
       nvl(android.interval_5, 0) AS android_interval_5_num,
       nvl(android.interval_6, 0) AS android_interval_6_num,
       nvl(android.interval_7, 0) AS android_interval_7_num
FROM (SELECT nvl(temp2.region_code, 'all')     AS region_code,
             nvl(temp2.main_channel, 'all')    AS main_channel,
             nvl(temp2.is_new_user, 'all')     AS is_new_user,
             nvl(temp2.is_new_activate, 'all') AS is_new_activate,
             nvl(temp2.pt, 'all')              AS event_date,
             count(DISTINCT temp2.next_0)      AS next_0,
             count(DISTINCT temp2.next_1)      AS next_1,
             count(DISTINCT temp2.next_3)      AS next_3,
             count(DISTINCT temp2.next_7)      AS next_7,
             count(DISTINCT temp2.next_28)     AS next_28,
             count(DISTINCT temp2.next_1)      AS interval_1,
             count(DISTINCT temp2.interval_2)  AS interval_2,
             count(DISTINCT temp2.interval_3)  AS interval_3,
             count(DISTINCT temp2.interval_4)  AS interval_4,
             count(DISTINCT temp2.interval_5)  AS interval_5,
             count(DISTINCT temp2.interval_6)  AS interval_6,
             count(DISTINCT temp2.interval_7)  AS interval_7
      FROM (
               SELECT temp.region_code,
                      temp.datasource,
                      temp.device_id,
                      if(temp.ddiff = 0, temp.device_id, NULL)                      AS next_0,
                      if(temp.ddiff = 1, temp.device_id, NULL)                      AS next_1,
                      if(temp.ddiff = 3, temp.device_id, NULL)                      AS next_3,
                      if(temp.ddiff = 7, temp.device_id, NULL)                      AS next_7,
                      if(temp.ddiff = 28, temp.device_id, NULL)                     AS next_28,
                      if(temp.ddiff >= 1 AND temp.ddiff <= 2, temp.device_id, NULL) AS interval_2,
                      if(temp.ddiff >= 1 AND temp.ddiff <= 3, temp.device_id, NULL) AS interval_3,
                      if(temp.ddiff >= 1 AND temp.ddiff <= 4, temp.device_id, NULL) AS interval_4,
                      if(temp.ddiff >= 1 AND temp.ddiff <= 5, temp.device_id, NULL) AS interval_5,
                      if(temp.ddiff >= 1 AND temp.ddiff <= 6, temp.device_id, NULL) AS interval_6,
                      if(temp.ddiff >= 1 AND temp.ddiff <= 7, temp.device_id, NULL) AS interval_7,
                      temp.main_channel,
                      temp.is_new_user,
                      temp.is_new_activate,
                      temp.pt
               FROM (SELECT nvl(su.region_code, 'NALL')                             AS region_code,
                            nvl(su.datasource, 'NALL')                              AS datasource,
                            nvl(su.device_id, 'NALL')                               AS device_id,
                            datediff(su2.pt, su.pt)                                 AS ddiff,
                            nvl(dd.main_channel, 'NALL')                            AS main_channel,
                            nvl(IF(dd.first_order_id is null, 'Y', 'N'), 'N') AS is_new_user,
                            nvl(IF(DATE(dd.activate_time) = su.pt, 'Y', 'N'), 'N')  AS is_new_activate,
                            su.pt
                     FROM dwd.dwd_vova_fact_start_up su
                              LEFT JOIN dim.dim_vova_devices dd ON su.device_id = dd.device_id
                              AND su.datasource = dd.datasource
                              INNER JOIN dwd.dwd_vova_fact_start_up su2 ON
                             su.device_id = su2.device_id
                             AND su.datasource = su2.datasource
                     WHERE su.pt = date_sub('${cur_date}', 28)
                      AND datediff(su2.pt, su.pt) in (0,1,2,3,4,5,6,7,28)
                      AND su.datasource not in ('vova', 'airyclub')
                    ) temp) temp2
      GROUP BY CUBE(temp2.region_code, temp2.main_channel,
                    temp2.is_new_user, temp2.is_new_activate, temp2.pt)
      HAVING event_date != 'all') AS total
         LEFT JOIN (
    SELECT nvl(temp2.region_code, 'all')     AS region_code,
           nvl(temp2.main_channel, 'all')    AS main_channel,
           nvl(temp2.is_new_user, 'all')     AS is_new_user,
           nvl(temp2.is_new_activate, 'all') AS is_new_activate,
           nvl(temp2.pt, 'all')              AS event_date,
           count(DISTINCT temp2.next_0)      AS next_0,
           count(DISTINCT temp2.next_1)      AS next_1,
           count(DISTINCT temp2.next_3)      AS next_3,
           count(DISTINCT temp2.next_7)      AS next_7,
           count(DISTINCT temp2.next_28)     AS next_28,
           count(DISTINCT temp2.next_1)      AS interval_1,
           count(DISTINCT temp2.interval_2)  AS interval_2,
           count(DISTINCT temp2.interval_3)  AS interval_3,
           count(DISTINCT temp2.interval_4)  AS interval_4,
           count(DISTINCT temp2.interval_5)  AS interval_5,
           count(DISTINCT temp2.interval_6)  AS interval_6,
           count(DISTINCT temp2.interval_7)  AS interval_7
    FROM (
             SELECT temp.region_code,
                    temp.datasource,
                    temp.device_id,
                    IF(temp.ddiff = 0, temp.device_id, NULL)                      AS next_0,
                    IF(temp.ddiff = 1, temp.device_id, NULL)                      AS next_1,
                    IF(temp.ddiff = 3, temp.device_id, NULL)                      AS next_3,
                    IF(temp.ddiff = 7, temp.device_id, NULL)                      AS next_7,
                    IF(temp.ddiff = 28, temp.device_id, NULL)                     AS next_28,
                    IF(temp.ddiff >= 1 AND temp.ddiff <= 2, temp.device_id, NULL) AS interval_2,
                    IF(temp.ddiff >= 1 AND temp.ddiff <= 3, temp.device_id, NULL) AS interval_3,
                    IF(temp.ddiff >= 1 AND temp.ddiff <= 4, temp.device_id, NULL) AS interval_4,
                    IF(temp.ddiff >= 1 AND temp.ddiff <= 5, temp.device_id, NULL) AS interval_5,
                    IF(temp.ddiff >= 1 AND temp.ddiff <= 6, temp.device_id, NULL) AS interval_6,
                    IF(temp.ddiff >= 1 AND temp.ddiff <= 7, temp.device_id, NULL) AS interval_7,
                    temp.main_channel,
                    temp.is_new_user,
                    temp.is_new_activate,
                    temp.pt
             FROM (SELECT nvl(su.region_code, 'NALL')                             AS region_code,
                          nvl(su.datasource, 'NALL')                              AS datasource,
                          nvl(su.device_id, 'NALL')                               AS device_id,
                          datediff(su2.pt, su.pt)                                 AS ddiff,
                          nvl(dd.main_channel, 'NALL')                            AS main_channel,
                          nvl(IF(dd.first_order_id is null, 'Y', 'N'), 'N') AS is_new_user,
                          nvl(IF(DATE(dd.activate_time) = su.pt, 'Y', 'N'), 'N')  AS is_new_activate,
                          su.pt
                   FROM dwd.dwd_vova_fact_start_up su
                            LEFT JOIN dim.dim_vova_devices dd ON su.device_id = dd.device_id
                            AND su.datasource = dd.datasource
                            INNER JOIN dwd.dwd_vova_fact_start_up su2 ON
                           su.device_id = su2.device_id
                           AND su.datasource = su2.datasource
                     WHERE su.pt = date_sub('${cur_date}', 28)
                      AND datediff(su2.pt, su.pt) in (0,1,2,3,4,5,6,7,28)
                     AND su.platform = 'ios'
                      AND su.datasource not in ('vova', 'airyclub')
                  ) temp) temp2
    GROUP BY CUBE(temp2.region_code, temp2.main_channel,
                  temp2.is_new_user, temp2.is_new_activate, temp2.pt)
    HAVING event_date != 'all') AS ios
                   ON total.region_code = ios.region_code
                       AND total.main_channel = ios.main_channel
                       AND total.is_new_user = ios.is_new_user
                       AND total.is_new_activate = ios.is_new_activate
                       AND total.event_date = ios.event_date
         LEFT JOIN (SELECT nvl(temp2.region_code, 'all')     AS region_code,
                           nvl(temp2.main_channel, 'all')    AS main_channel,
                           nvl(temp2.is_new_user, 'all')     AS is_new_user,
                           nvl(temp2.is_new_activate, 'all') AS is_new_activate,
                           nvl(temp2.pt, 'all')              AS event_date,
                           count(DISTINCT temp2.next_0)      AS next_0,
                           count(DISTINCT temp2.next_1)      AS next_1,
                           count(DISTINCT temp2.next_3)      AS next_3,
                           count(DISTINCT temp2.next_7)      AS next_7,
                           count(DISTINCT temp2.next_28)     AS next_28,
                           count(DISTINCT temp2.next_1)      AS interval_1,
                           count(DISTINCT temp2.interval_2)  AS interval_2,
                           count(DISTINCT temp2.interval_3)  AS interval_3,
                           count(DISTINCT temp2.interval_4)  AS interval_4,
                           count(DISTINCT temp2.interval_5)  AS interval_5,
                           count(DISTINCT temp2.interval_6)  AS interval_6,
                           count(DISTINCT temp2.interval_7)  AS interval_7
                    FROM (
                             SELECT temp.region_code,
                                    temp.datasource,
                                    temp.device_id,
                                    IF(temp.ddiff = 0, temp.device_id, NULL)                      AS next_0,
                                    IF(temp.ddiff = 1, temp.device_id, NULL)                      AS next_1,
                                    IF(temp.ddiff = 3, temp.device_id, NULL)                      AS next_3,
                                    IF(temp.ddiff = 7, temp.device_id, NULL)                      AS next_7,
                                    IF(temp.ddiff = 28, temp.device_id, NULL)                     AS next_28,
                                    IF(temp.ddiff >= 1 AND temp.ddiff <= 2, temp.device_id, NULL) AS interval_2,
                                    IF(temp.ddiff >= 1 AND temp.ddiff <= 3, temp.device_id, NULL) AS interval_3,
                                    IF(temp.ddiff >= 1 AND temp.ddiff <= 4, temp.device_id, NULL) AS interval_4,
                                    IF(temp.ddiff >= 1 AND temp.ddiff <= 5, temp.device_id, NULL) AS interval_5,
                                    IF(temp.ddiff >= 1 AND temp.ddiff <= 6, temp.device_id, NULL) AS interval_6,
                                    IF(temp.ddiff >= 1 AND temp.ddiff <= 7, temp.device_id, NULL) AS interval_7,
                                    temp.main_channel,
                                    temp.is_new_user,
                                    temp.is_new_activate,
                                    temp.pt
                             FROM (SELECT nvl(su.region_code, 'NALL')                             AS region_code,
                                          nvl(su.datasource, 'NALL')                              AS datasource,
                                          nvl(su.device_id, 'NALL')                               AS device_id,
                                          datediff(su2.pt, su.pt)                                 AS ddiff,
                                          nvl(dd.main_channel, 'NALL')                            AS main_channel,
                                          nvl(IF(dd.first_order_id is null, 'Y', 'N'), 'N') AS is_new_user,
                                          nvl(IF(DATE(dd.activate_time) = su.pt, 'Y', 'N'), 'N')  AS is_new_activate,
                                          su.pt
                                   FROM dwd.dwd_vova_fact_start_up su
                                            LEFT JOIN dim.dim_vova_devices dd ON su.device_id = dd.device_id
                                            AND su.datasource = dd.datasource
                                            INNER JOIN dwd.dwd_vova_fact_start_up su2 ON
                                           su.device_id = su2.device_id
                                           AND su.datasource = su2.datasource
                     WHERE su.pt = date_sub('${cur_date}', 28)
                      AND datediff(su2.pt, su.pt) in (0,1,2,3,4,5,6,7,28)
                                     AND su.platform = 'android'
                      AND su.datasource not in ('vova', 'airyclub')
                                  ) temp) temp2
                    GROUP BY CUBE(temp2.region_code, temp2.main_channel,
                                  temp2.is_new_user, temp2.is_new_activate, temp2.pt)
                    HAVING event_date != 'all') AS android
                   ON total.region_code = android.region_code
                       AND total.main_channel = android.main_channel
                       AND total.is_new_user = android.is_new_user
                       AND total.is_new_activate = android.is_new_activate
                       AND total.event_date = android.event_date
;
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=${job_name}" \
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
echo "end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

# https://sqoop.apache.org/docs/1.4.2/SqoopUserGuide.html
sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=1 \
-Dsqoop.export.statements.per.transaction=1 \
-Dmapreduce.job.queuename=default \
--connect jdbc:mariadb:aurora://db-logistics-w.gitvv.com:3306/themis_logistics_report?rewriteBatchedStatements=true \
--username vvreport4vv --password nTTPdJhVp!DGv5VX4z33Fw@tHLmIG8oS --connection-manager org.apache.sqoop.manager.MySQLManager \
--table rpt_user_cohort \
--update-key "event_date,datasource,region_code,main_channel,is_new_user,is_new_activate" \
--update-mode allowinsert \
--hcatalog-database dwb \
--hcatalog-table dwb_vova_user_cohort \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${cur_date} \
--fields-terminated-by '\001' \
--batch

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi


