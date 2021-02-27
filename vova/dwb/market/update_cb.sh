#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

sql="
insert overwrite table dwb.dwb_vova_market_cb_region_filter
SELECT
    /*+ REPARTITION(1) */
    'vova'                   AS datasource,
    tmp1.event_date,
    tmp1.region_code,
    tmp2.paid_num / tmp1.dau AS cr
FROM (
         SELECT su.pt AS event_date,
                su.region_code,
                count(DISTINCT device_id) AS dau
         FROM dwd.dwd_vova_fact_start_up su
         WHERE su.datasource = 'vova'
           AND su.pt >= date_sub('${cur_date}', 60)
         GROUP BY su.pt, su.region_code
     ) AS tmp1
         INNER JOIN (
    SELECT date(fp.pay_time)            AS event_date,
           fp.region_code,
           count(DISTINCT fp.device_id) AS paid_num
    FROM dwd.dwd_vova_fact_pay fp
    WHERE fp.datasource = 'vova'
    GROUP BY date(fp.pay_time), fp.region_code
) AS tmp2 ON tmp1.event_date = tmp2.event_date AND tmp1.region_code = tmp2.region_code
WHERE tmp2.paid_num / tmp1.dau > 0.015
  AND tmp1.region_code NOT IN ('BR', 'IN', 'ID');

REFRESH table dwb.dwb_vova_market_cb_region_filter;

INSERT overwrite TABLE dwb.dwb_vova_market_cb PARTITION (pt = '${cur_date}')
SELECT /*+ REPARTITION(1) */
       tot_dau.event_date,
       tot_dau.datasource,
       tot_dau.region_code,
       tot_gmv,
       tot_dau,
       tot_install,
       android_paid_device,
       android_gmv,
       android_dau,
       android_install,
       ios_paid_device,
       ios_gmv,
       ios_dau,
       ios_install,
       tot_android_1b_ret,
       tot_android_7b_ret,
       tot_android_28b_ret,
       tot_ios_1b_ret,
       tot_ios_7b_ret,
       tot_ios_28b_ret,
       new_android_1b_ret,
       new_android_7b_ret,
       new_android_28b_ret,
       new_ios_1b_ret,
       new_ios_7b_ret,
       new_ios_28b_ret,
       tot_activate,
       android_activate,
       ios_activate
FROM

    -- dau
    (SELECT nvl(temp.event_date, 'all')     event_date,
            count(DISTINCT temp.device_id)  tot_dau,
            nvl(temp.datasource, 'all')  AS datasource,
            nvl(temp.region_code, 'all') AS region_code
     FROM (SELECT nvl(su.pt, 'NALL')             event_date,
                  su.device_id,
                  nvl(su.datasource, 'NALL')  AS datasource,
                  nvl(su.region_code, 'NALL') AS region_code
           FROM dwd.dwd_vova_fact_start_up su
                    INNER JOIN dwb.dwb_vova_market_cb_region_filter ccr
                               ON su.region_code = ccr.region_code AND su.pt = ccr.event_date
           WHERE su.pt > date_sub('${cur_date}', 60)) temp
     GROUP BY CUBE(temp.event_date, temp.datasource, temp.region_code)
    ) tot_dau

        LEFT JOIN (
        SELECT nvl(temp.event_date, 'all')     event_date,
               count(DISTINCT temp.device_id)  android_dau,
               nvl(temp.datasource, 'all')  AS datasource,
               nvl(temp.region_code, 'all') AS region_code
        FROM (SELECT nvl(su.pt, 'NALL')             event_date,
                     su.device_id,
                     nvl(su.datasource, 'NALL')  AS datasource,
                     nvl(su.region_code, 'NALL') AS region_code
              FROM dwd.dwd_vova_fact_start_up su
                       INNER JOIN dwb.dwb_vova_market_cb_region_filter ccr
                                  ON su.region_code = ccr.region_code AND su.pt = ccr.event_date
              WHERE su.pt > date_sub('${cur_date}', 60)
                AND su.platform = 'android'
             ) temp

        GROUP BY CUBE(temp.event_date, temp.datasource, temp.region_code)
    ) android_dau
                  ON android_dau.event_date = tot_dau.event_date
                      AND android_dau.region_code = tot_dau.region_code
                      AND android_dau.datasource = tot_dau.datasource

        LEFT JOIN (
        SELECT nvl(temp.event_date, 'all')     event_date,
               count(DISTINCT temp.device_id)  ios_dau,
               nvl(temp.datasource, 'all')  AS datasource,
               nvl(temp.region_code, 'all') AS region_code
        FROM (SELECT nvl(su.pt, 'NALL')             event_date,
                     su.device_id,
                     nvl(su.datasource, 'NALL')  AS datasource,
                     nvl(su.region_code, 'NALL') AS region_code
              FROM dwd.dwd_vova_fact_start_up su
                       INNER JOIN dwb.dwb_vova_market_cb_region_filter ccr
                                  ON su.region_code = ccr.region_code AND su.pt = ccr.event_date
              WHERE su.pt > date_sub('${cur_date}', 60)
                AND su.platform = 'ios'
             ) temp

        GROUP BY CUBE(temp.event_date, temp.datasource, temp.region_code)
    ) ios_dau
                  ON ios_dau.event_date = tot_dau.event_date
                      AND ios_dau.region_code = tot_dau.region_code
                      AND ios_dau.datasource = tot_dau.datasource
        -- install
        LEFT JOIN (
        SELECT nvl(temp.event_date, 'all')     event_date,
               count(DISTINCT temp.device_id)  tot_install,
               nvl(temp.datasource, 'all')  AS datasource,
               nvl(temp.region_code, 'all') AS region_code
        FROM (SELECT nvl(date(install_time), 'NALL') event_date,
                     dev.device_id,
                     nvl(dev.datasource, 'NALL')  AS     datasource,
                     nvl(dev.region_code, 'NALL') AS     region_code
              FROM dim.dim_vova_devices dev
                       INNER JOIN dwb.dwb_vova_market_cb_region_filter ccr
                                  ON dev.region_code = ccr.region_code AND date(dev.install_time) = ccr.event_date
              WHERE install_time > date_sub('${cur_date}', 60)) temp
        GROUP BY CUBE(temp.event_date, temp.datasource, temp.region_code)
    ) tot_install
                  ON tot_install.event_date = tot_dau.event_date
                      AND tot_install.region_code = tot_dau.region_code
                      AND tot_install.datasource = tot_dau.datasource
        LEFT JOIN (
        SELECT nvl(temp.event_date, 'all')     event_date,
               count(DISTINCT temp.device_id)  android_install,
               nvl(temp.datasource, 'all')  AS datasource,
               nvl(temp.region_code, 'all') AS region_code
        FROM (SELECT nvl(date(install_time), 'NALL') event_date,
                     dev.device_id,
                     nvl(dev.datasource, 'NALL')  AS     datasource,
                     nvl(dev.region_code, 'NALL') AS     region_code
              FROM dim.dim_vova_devices dev
                       INNER JOIN dwb.dwb_vova_market_cb_region_filter ccr
                                  ON dev.region_code = ccr.region_code AND date(dev.install_time) = ccr.event_date
              WHERE install_time > date_sub('${cur_date}', 60)
                AND dev.platform = 'android'
             ) temp
        GROUP BY CUBE(temp.event_date, temp.datasource, temp.region_code)
    ) android_install
                  ON android_install.event_date = tot_dau.event_date
                      AND android_install.region_code = tot_dau.region_code
                      AND android_install.datasource = tot_dau.datasource

        LEFT JOIN (
        SELECT nvl(temp.event_date, 'all')     event_date,
               count(DISTINCT temp.device_id)  ios_install,
               nvl(temp.datasource, 'all')  AS datasource,
               nvl(temp.region_code, 'all') AS region_code
        FROM (SELECT nvl(date(install_time), 'NALL') event_date,
                     dev.device_id,
                     nvl(dev.datasource, 'NALL')  AS     datasource,
                     nvl(dev.region_code, 'NALL') AS     region_code
              FROM dim.dim_vova_devices dev
                       INNER JOIN dwb.dwb_vova_market_cb_region_filter ccr
                                  ON dev.region_code = ccr.region_code AND date(dev.install_time) = ccr.event_date
              WHERE install_time > date_sub('${cur_date}', 60)
                AND dev.platform = 'ios'
             ) temp
        GROUP BY CUBE(temp.event_date, temp.datasource, temp.region_code)
    ) ios_install
                  ON ios_install.event_date = tot_dau.event_date
                      AND ios_install.region_code = tot_dau.region_code
                      AND ios_install.datasource = tot_dau.datasource

        -- activate data
        LEFT JOIN (
        SELECT nvl(temp.event_date, 'all')     event_date,
               count(DISTINCT temp.device_id)  tot_activate,
               nvl(temp.datasource, 'all')  AS datasource,
               nvl(temp.region_code, 'all') AS region_code
        FROM (SELECT nvl(date(activate_time), 'NALL') event_date,
                     dev.device_id,
                     nvl(dev.datasource, 'NALL')  AS      datasource,
                     nvl(dev.region_code, 'NALL') AS      region_code
              FROM dim.dim_vova_devices dev
                       INNER JOIN dwb.dwb_vova_market_cb_region_filter ccr
                                  ON dev.region_code = ccr.region_code AND date(dev.activate_time) = ccr.event_date
              WHERE activate_time IS NOT NULL
                AND activate_time > date_sub('${cur_date}', 60)) temp
        GROUP BY CUBE(temp.event_date, temp.datasource, temp.region_code)
    ) tot_activate
                  ON tot_activate.event_date = tot_dau.event_date
                      AND tot_activate.region_code = tot_dau.region_code
                      AND tot_activate.datasource = tot_dau.datasource

        LEFT JOIN (
        SELECT nvl(temp.event_date, 'all')     event_date,
               count(DISTINCT temp.device_id)  android_activate,
               nvl(temp.datasource, 'all')  AS datasource,
               nvl(temp.region_code, 'all') AS region_code
        FROM (SELECT nvl(date(activate_time), 'NALL') event_date,
                     dev.device_id,
                     nvl(dev.datasource, 'NALL')  AS      datasource,
                     nvl(dev.region_code, 'NALL') AS      region_code
              FROM dim.dim_vova_devices dev
                       INNER JOIN dwb.dwb_vova_market_cb_region_filter ccr
                                  ON dev.region_code = ccr.region_code AND date(dev.activate_time) = ccr.event_date
              WHERE activate_time IS NOT NULL
                AND activate_time > date_sub('${cur_date}', 60)
                AND platform = 'android'
             ) temp
        GROUP BY CUBE(temp.event_date, temp.datasource, temp.region_code)
    ) android_activate
                  ON android_activate.event_date = tot_dau.event_date
                      AND android_activate.region_code = tot_dau.region_code
                      AND android_activate.datasource = tot_dau.datasource

        LEFT JOIN (
        SELECT nvl(temp.event_date, 'all')     event_date,
               count(DISTINCT temp.device_id)  ios_activate,
               nvl(temp.datasource, 'all')  AS datasource,
               nvl(temp.region_code, 'all') AS region_code
        FROM (SELECT nvl(date(activate_time), 'NALL') event_date,
                     dev.device_id,
                     nvl(dev.datasource, 'NALL')  AS      datasource,
                     nvl(dev.region_code, 'NALL') AS      region_code
              FROM dim.dim_vova_devices dev
                       INNER JOIN dwb.dwb_vova_market_cb_region_filter ccr
                                  ON dev.region_code = ccr.region_code AND date(dev.activate_time) = ccr.event_date
              WHERE activate_time IS NOT NULL
                AND activate_time > date_sub('${cur_date}', 60)
                AND platform = 'ios'
             ) temp
        GROUP BY CUBE(temp.event_date, temp.datasource, temp.region_code)
    ) ios_activate
                  ON ios_activate.event_date = tot_dau.event_date
                      AND ios_activate.region_code = tot_dau.region_code
                      AND ios_activate.datasource = tot_dau.datasource

        -- gmv
        LEFT JOIN (
        SELECT nvl(temp.event_date, 'all')     event_date,
               sum(temp.tot_gmv)               tot_gmv,
               nvl(temp.datasource, 'all')  AS datasource,
               nvl(temp.region_code, 'all') AS region_code
        FROM (SELECT nvl(date(pay_time), 'NALL')                 event_date,
                     goods_number * shop_price + shipping_fee AS tot_gmv,
                     nvl(fp.datasource, 'NALL')                  AS datasource,
                     nvl(fp.region_code, 'NALL')                 AS region_code
              FROM dwd.dwd_vova_fact_pay fp
                       INNER JOIN dwb.dwb_vova_market_cb_region_filter ccr
                                  ON fp.region_code = ccr.region_code AND date(fp.pay_time) = ccr.event_date
              WHERE fp.pay_time > date_sub('${cur_date}', 60)
             ) temp
        GROUP BY CUBE(temp.event_date, temp.datasource, temp.region_code)
    ) tot_gmv
                  ON tot_gmv.event_date = tot_dau.event_date
                      AND tot_gmv.region_code = tot_dau.region_code
                      AND tot_gmv.datasource = tot_dau.datasource


        LEFT JOIN (
        SELECT nvl(temp.event_date, 'all')       event_date,
               sum(temp.android_gmv)             android_gmv,
               count(DISTINCT temp.device_id) AS android_paid_device,
               nvl(temp.datasource, 'all')    AS datasource,
               nvl(temp.region_code, 'all')   AS region_code
        FROM (SELECT nvl(date(pay_time), 'NALL')                 event_date,
                     goods_number * shop_price + shipping_fee AS android_gmv,
                     fp.device_id,
                     nvl(fp.datasource, 'NALL')                  AS datasource,
                     nvl(fp.region_code, 'NALL')                 AS region_code
              FROM dwd.dwd_vova_fact_pay fp
                       INNER JOIN dwb.dwb_vova_market_cb_region_filter ccr
                                  ON fp.region_code = ccr.region_code AND date(fp.pay_time) = ccr.event_date
              WHERE fp.pay_time > date_sub('${cur_date}', 60)
                AND fp.platform = 'android'
             ) temp
        GROUP BY CUBE(temp.event_date, temp.datasource, temp.region_code)
    ) android_gmv
                  ON android_gmv.event_date = tot_dau.event_date
                      AND android_gmv.region_code = tot_dau.region_code
                      AND android_gmv.datasource = tot_dau.datasource


        LEFT JOIN (
        SELECT nvl(temp.event_date, 'all')       event_date,
               sum(temp.ios_gmv)                 ios_gmv,
               count(DISTINCT temp.device_id) AS ios_paid_device,
               nvl(temp.datasource, 'all')    AS datasource,
               nvl(temp.region_code, 'all')   AS region_code
        FROM (SELECT nvl(date(pay_time), 'NALL')                 event_date,
                     goods_number * shop_price + shipping_fee AS ios_gmv,
                     fp.device_id,
                     nvl(fp.datasource, 'NALL')                  AS datasource,
                     nvl(fp.region_code, 'NALL')                 AS region_code
              FROM dwd.dwd_vova_fact_pay fp
                       INNER JOIN dwb.dwb_vova_market_cb_region_filter ccr
                                  ON fp.region_code = ccr.region_code AND date(fp.pay_time) = ccr.event_date
              WHERE fp.pay_time > date_sub('${cur_date}', 60)
                AND fp.platform = 'ios'
             ) temp
        GROUP BY CUBE(temp.event_date, temp.datasource, temp.region_code)
    ) ios_gmv
                  ON ios_gmv.event_date = tot_dau.event_date
                      AND ios_gmv.region_code = tot_dau.region_code
                      AND ios_gmv.datasource = tot_dau.datasource


        -- activate user 1 day before cohort
        LEFT JOIN (
        SELECT nvl(su.pt, 'all')             event_date,
               count(DISTINCT su.device_id)  new_android_1b_ret,
                          nvl(nvl(su.datasource, 'NA'), 'all')  AS datasource,
                          nvl(nvl(su.region_code, 'NALL'), 'all') AS region_code
        FROM dwd.dwd_vova_fact_start_up su
                 INNER JOIN dim.dim_vova_devices dd ON dd.device_id = su.device_id
                 INNER JOIN dwb.dwb_vova_market_cb_region_filter ccr
                            ON su.region_code = ccr.region_code AND su.pt = ccr.event_date
                                AND dd.activate_time IS NOT NULL
                                AND su.datasource = dd.datasource
        WHERE su.pt > date_sub('${cur_date}', 60)
          AND datediff(su.pt, date(dd.activate_time)) = 1
          AND su.platform = 'android'
        GROUP BY CUBE(su.pt, nvl(su.datasource, 'NA'), nvl(su.region_code, 'NALL'))
    ) new_android_1b_cohort
                  ON new_android_1b_cohort.event_date = tot_dau.event_date
                      AND new_android_1b_cohort.region_code = tot_dau.region_code
                      AND new_android_1b_cohort.datasource = tot_dau.datasource

        LEFT JOIN (SELECT nvl(su.pt, 'all')             event_date,
                          count(DISTINCT su.device_id)  new_ios_1b_ret,
                          nvl(nvl(su.datasource, 'NA'), 'all')  AS datasource,
                          nvl(nvl(su.region_code, 'NALL'), 'all') AS region_code
                   FROM dwd.dwd_vova_fact_start_up su
                            INNER JOIN dim.dim_vova_devices dd ON dd.device_id = su.device_id
                            INNER JOIN dwb.dwb_vova_market_cb_region_filter ccr
                                       ON su.region_code = ccr.region_code AND su.pt = ccr.event_date
                                           AND dd.activate_time IS NOT NULL
                                           AND su.datasource = dd.datasource
                   WHERE su.pt > date_sub('${cur_date}', 60)
                     AND datediff(su.pt, date(dd.activate_time)) = 1
                     AND su.platform = 'ios'
                   GROUP BY CUBE(su.pt, nvl(su.datasource, 'NA'), nvl(su.region_code, 'NALL'))
    ) new_ios_1b_cohort
                  ON new_ios_1b_cohort.event_date = tot_dau.event_date
                      AND new_ios_1b_cohort.region_code = tot_dau.region_code
                      AND new_ios_1b_cohort.datasource = tot_dau.datasource

        -- activate user 7 day before cohort
        LEFT JOIN (SELECT nvl(su.pt, 'all')             event_date,
                          count(DISTINCT su.device_id)  new_android_7b_ret,
                          nvl(nvl(su.datasource, 'NA'), 'all')  AS datasource,
                          nvl(nvl(su.region_code, 'NALL'), 'all') AS region_code
                   FROM dwd.dwd_vova_fact_start_up su
                            INNER JOIN dim.dim_vova_devices dd ON dd.device_id = su.device_id
                            INNER JOIN dwb.dwb_vova_market_cb_region_filter ccr
                                       ON su.region_code = ccr.region_code AND su.pt = ccr.event_date
                                           AND dd.activate_time IS NOT NULL
                                           AND su.datasource = dd.datasource
                   WHERE su.pt > date_sub('${cur_date}', 60)
                     AND datediff(su.pt, date(dd.activate_time)) = 7
                     AND su.platform = 'android'
                   GROUP BY CUBE(su.pt, nvl(su.datasource, 'NA'), nvl(su.region_code, 'NALL'))
    ) new_android_7b_cohort
                  ON new_android_7b_cohort.event_date = tot_dau.event_date
                      AND new_android_7b_cohort.region_code = tot_dau.region_code
                      AND new_android_7b_cohort.datasource = tot_dau.datasource
        LEFT JOIN (SELECT nvl(su.pt, 'all')             event_date,
                          count(DISTINCT su.device_id)  new_ios_7b_ret,
                          nvl(nvl(su.datasource, 'NA'), 'all')  AS datasource,
                          nvl(nvl(su.region_code, 'NALL'), 'all') AS region_code
                   FROM dwd.dwd_vova_fact_start_up su
                            INNER JOIN dim.dim_vova_devices dd ON dd.device_id = su.device_id
                            INNER JOIN dwb.dwb_vova_market_cb_region_filter ccr
                                       ON su.region_code = ccr.region_code AND su.pt = ccr.event_date
                                           AND dd.activate_time IS NOT NULL
                                           AND su.datasource = dd.datasource
                   WHERE su.pt > date_sub('${cur_date}', 60)
                     AND datediff(su.pt, date(dd.activate_time)) = 7
                     AND su.platform = 'ios'
                   GROUP BY CUBE(su.pt, nvl(su.datasource, 'NA'), nvl(su.region_code, 'NALL'))
    ) new_ios_7b_cohort
                  ON new_ios_7b_cohort.event_date = tot_dau.event_date
                      AND new_ios_7b_cohort.region_code = tot_dau.region_code
                      AND new_ios_7b_cohort.datasource = tot_dau.datasource
        -- activate user 28 day before cohort
        LEFT JOIN (SELECT nvl(su.pt, 'all')             event_date,
                          count(DISTINCT su.device_id)  new_android_28b_ret,
                          nvl(nvl(su.datasource, 'NA'), 'all')  AS datasource,
                          nvl(nvl(su.region_code, 'NALL'), 'all') AS region_code
                   FROM dwd.dwd_vova_fact_start_up su
                            INNER JOIN dim.dim_vova_devices dd ON dd.device_id = su.device_id
                            INNER JOIN dwb.dwb_vova_market_cb_region_filter ccr
                                       ON su.region_code = ccr.region_code AND su.pt = ccr.event_date
                                           AND dd.activate_time IS NOT NULL
                                           AND su.datasource = dd.datasource
                   WHERE su.pt > date_sub('${cur_date}', 60)
                     AND datediff(su.pt, date(dd.activate_time)) = 28
                     AND su.platform = 'android'
                   GROUP BY CUBE(su.pt, nvl(su.datasource, 'NA'), nvl(su.region_code, 'NALL'))
    ) new_android_28b_cohort
                  ON new_android_28b_cohort.event_date = tot_dau.event_date
                      AND new_android_28b_cohort.region_code = tot_dau.region_code
                      AND new_android_28b_cohort.datasource = tot_dau.datasource
        LEFT JOIN (SELECT nvl(su.pt, 'all')             event_date,
                          count(DISTINCT su.device_id)  new_ios_28b_ret,
                          nvl(nvl(su.datasource, 'NA'), 'all')  AS datasource,
                          nvl(nvl(su.region_code, 'NALL'), 'all') AS region_code
                   FROM dwd.dwd_vova_fact_start_up su
                            INNER JOIN dim.dim_vova_devices dd ON dd.device_id = su.device_id
                            INNER JOIN dwb.dwb_vova_market_cb_region_filter ccr
                                       ON su.region_code = ccr.region_code AND su.pt = ccr.event_date
                                           AND dd.activate_time IS NOT NULL
                                           AND su.datasource = dd.datasource
                   WHERE su.pt > date_sub('${cur_date}', 60)
                     AND datediff(su.pt, date(dd.activate_time)) = 28
                     AND su.platform = 'ios'
                   GROUP BY CUBE(su.pt, nvl(su.datasource, 'NA'), nvl(su.region_code, 'NALL'))
    ) new_ios_28b_cohort
                  ON new_ios_28b_cohort.event_date = tot_dau.event_date
                      AND new_ios_28b_cohort.region_code = tot_dau.region_code
                      AND new_ios_28b_cohort.datasource = tot_dau.datasource
        -- all_user 1 day before cohort
        LEFT JOIN (SELECT nvl(csu1.pt, 'all')             event_date,
                          count(DISTINCT csu1.device_id)  tot_android_1b_ret,
                          nvl(nvl(csu1.datasource, 'NA'), 'all')  AS datasource,
                          nvl(nvl(csu1.region_code, 'NALL'), 'all') AS region_code
                   FROM dwd.dwd_vova_fact_start_up csu1
                            INNER JOIN dwd.dwd_vova_fact_start_up csu2 ON csu1.device_id = csu2.device_id
                            INNER JOIN dwb.dwb_vova_market_cb_region_filter ccr
                                       ON csu1.region_code = ccr.region_code AND csu1.pt = ccr.event_date
                                           AND csu1.datasource = csu2.datasource
                   WHERE csu1.pt > date_sub('${cur_date}', 60)
                     AND datediff(csu1.pt, csu2.pt) = 1
                     AND csu1.platform = 'android'
                   GROUP BY CUBE(csu1.pt, nvl(csu1.datasource, 'NA'), nvl(csu1.region_code, 'NALL'))
    ) tot_android_1b_cohort
                  ON tot_android_1b_cohort.event_date = tot_dau.event_date
                      AND tot_android_1b_cohort.region_code = tot_dau.region_code
                      AND tot_android_1b_cohort.datasource = tot_dau.datasource
        LEFT JOIN (SELECT nvl(csu1.pt, 'all')             event_date,
                          count(DISTINCT csu1.device_id)  tot_ios_1b_ret,
                          nvl(nvl(csu1.datasource, 'NA'), 'all')  AS datasource,
                          nvl(nvl(csu1.region_code, 'NALL'), 'all') AS region_code
                   FROM dwd.dwd_vova_fact_start_up csu1
                            INNER JOIN dwd.dwd_vova_fact_start_up csu2 ON csu1.device_id = csu2.device_id
                            INNER JOIN dwb.dwb_vova_market_cb_region_filter ccr
                                       ON csu1.region_code = ccr.region_code AND csu1.pt = ccr.event_date
                                           AND csu1.datasource = csu2.datasource
                   WHERE csu1.pt > date_sub('${cur_date}', 60)
                     AND datediff(csu1.pt, csu2.pt) = 1
                     AND csu1.platform = 'ios'
                   GROUP BY CUBE(csu1.pt, nvl(csu1.datasource, 'NA'), nvl(csu1.region_code, 'NALL'))
    ) tot_ios_1b_cohort
                  ON tot_ios_1b_cohort.event_date = tot_dau.event_date
                      AND tot_ios_1b_cohort.region_code = tot_dau.region_code
                      AND tot_ios_1b_cohort.datasource = tot_dau.datasource
        -- all_user 7 day before cohort
        LEFT JOIN (SELECT nvl(csu1.pt, 'all')             event_date,
                          count(DISTINCT csu1.device_id)  tot_android_7b_ret,
                          nvl(nvl(csu1.datasource, 'NA'), 'all')  AS datasource,
                          nvl(nvl(csu1.region_code, 'NALL'), 'all') AS region_code
                   FROM dwd.dwd_vova_fact_start_up csu1
                            INNER JOIN dwd.dwd_vova_fact_start_up csu2 ON csu1.device_id = csu2.device_id
                            INNER JOIN dwb.dwb_vova_market_cb_region_filter ccr
                                       ON csu1.region_code = ccr.region_code AND csu1.pt = ccr.event_date
                                           AND csu1.datasource = csu2.datasource
                   WHERE csu1.pt > date_sub('${cur_date}', 60)
                     AND datediff(csu1.pt, csu2.pt) = 7
                     AND csu1.platform = 'android'
                   GROUP BY CUBE(csu1.pt, nvl(csu1.datasource, 'NA'), nvl(csu1.region_code, 'NALL'))
    ) tot_android_7b_cohort
                  ON tot_android_7b_cohort.event_date = tot_dau.event_date
                      AND tot_android_7b_cohort.region_code = tot_dau.region_code
                      AND tot_android_7b_cohort.datasource = tot_dau.datasource
        LEFT JOIN (SELECT nvl(csu1.pt, 'all')             event_date,
                          count(DISTINCT csu1.device_id)  tot_ios_7b_ret,
                          nvl(nvl(csu1.datasource, 'NA'), 'all')  AS datasource,
                          nvl(nvl(csu1.region_code, 'NALL'), 'all') AS region_code
                   FROM dwd.dwd_vova_fact_start_up csu1
                            INNER JOIN dwd.dwd_vova_fact_start_up csu2 ON csu1.device_id = csu2.device_id
                            INNER JOIN dwb.dwb_vova_market_cb_region_filter ccr
                                       ON csu1.region_code = ccr.region_code AND csu1.pt = ccr.event_date
                                           AND csu1.datasource = csu2.datasource
                   WHERE csu1.pt > date_sub('${cur_date}', 60)
                     AND datediff(csu1.pt, csu2.pt) = 7
                     AND csu1.platform = 'ios'
                   GROUP BY CUBE(csu1.pt, nvl(csu1.datasource, 'NA'), nvl(csu1.region_code, 'NALL'))
    ) tot_ios_7b_cohort
                  ON tot_ios_7b_cohort.event_date = tot_dau.event_date
                      AND tot_ios_7b_cohort.region_code = tot_dau.region_code
                      AND tot_ios_7b_cohort.datasource = tot_dau.datasource
        -- all_user 28 day before cohort
        LEFT JOIN (SELECT nvl(csu1.pt, 'all')             event_date,
                          count(DISTINCT csu1.device_id)  tot_android_28b_ret,
                          nvl(nvl(csu1.datasource, 'NA'), 'all')  AS datasource,
                          nvl(nvl(csu1.region_code, 'NALL'), 'all') AS region_code
                   FROM dwd.dwd_vova_fact_start_up csu1
                            INNER JOIN dwd.dwd_vova_fact_start_up csu2 ON csu1.device_id = csu2.device_id
                            INNER JOIN dwb.dwb_vova_market_cb_region_filter ccr
                                       ON csu1.region_code = ccr.region_code AND csu1.pt = ccr.event_date
                                           AND csu1.datasource = csu2.datasource
                   WHERE csu1.pt > date_sub('${cur_date}', 60)
                     AND datediff(csu1.pt, csu2.pt) = 28
                     AND csu1.platform = 'android'
                   GROUP BY CUBE(csu1.pt, nvl(csu1.datasource, 'NA'), nvl(csu1.region_code, 'NALL'))
    ) tot_android_28b_cohort
                  ON tot_android_28b_cohort.event_date = tot_dau.event_date
                      AND tot_android_28b_cohort.region_code = tot_dau.region_code
                      AND tot_android_28b_cohort.datasource = tot_dau.datasource
        LEFT JOIN (SELECT nvl(csu1.pt, 'all')             event_date,
                          count(DISTINCT csu1.device_id)  tot_ios_28b_ret,
                          nvl(nvl(csu1.datasource, 'NA'), 'all')  AS datasource,
                          nvl(nvl(csu1.region_code, 'NALL'), 'all') AS region_code
                   FROM dwd.dwd_vova_fact_start_up csu1
                            INNER JOIN dwd.dwd_vova_fact_start_up csu2 ON csu1.device_id = csu2.device_id
                            INNER JOIN dwb.dwb_vova_market_cb_region_filter ccr
                                       ON csu1.region_code = ccr.region_code AND csu1.pt = ccr.event_date
                                           AND csu1.datasource = csu2.datasource
                   WHERE csu1.pt > date_sub('${cur_date}', 60)
                     AND datediff(csu1.pt, csu2.pt) = 28
                     AND csu1.platform = 'ios'
                   GROUP BY CUBE(csu1.pt, nvl(csu1.datasource, 'NA'), nvl(csu1.region_code, 'NALL'))
    ) tot_ios_28b_cohort
                  ON tot_ios_28b_cohort.event_date = tot_dau.event_date
                      AND tot_ios_28b_cohort.region_code = tot_dau.region_code
                      AND tot_ios_28b_cohort.datasource = tot_dau.datasource
WHERE tot_dau.event_date != 'all';
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=dwb_vova_market_cb" \
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