set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

drop table tmp.tmp_dwb_fd_ad_cost_gmv;
create table tmp.tmp_dwb_fd_ad_cost_gmv as
select /*+ REPARTITION(1) */
    nvl(date, 'all')                                              AS event_date,
    nvl(if(ads_site_code = 'FD', 'floryday', 'airydress'), 'all') AS project_name,
    nvl(ga_channel, 'all')                                        AS ga_channel,
    nvl(r.region_code, 'all')                                     AS country_code,
    nvl(platform, 'all')                                          AS platform_type,
    sum(cost)                                                     AS tot_cost,
    sum(gmv)                                                      AS tot_gmv
from (
         select date,
                ads_site_code,
                country,
                ga_channel,
                case
                    when ga_channel regexp "ios" then "ios_app"
                    when ga_channel regexp "android" then "android_app"
                    when ga_channel in ("app_dr_api","api_other") then "android_app"
                    else "others"
                    end as platform,
                cost,
                0       as gmv

         from ods_fd_ar.ods_fd_ads_ga_channel_daily_flat_report

         union all
         select date,
                ads_site_code,
                country,
                ga_channel,
                case
                    when ga_channel regexp 'ios' then 'ios_app'
                    when ga_channel regexp 'android' then 'android_app'
                    when ga_channel in ("app_dr_api","api_other") then 'android_app'
                    when ga_channel in ("apple_search_api") then 'ios_app'
                    else 'others' end as platform,
                0                     as cost,
                gmv
         from ods_fd_ar.ods_fd_ads_ga_channel_daily_gmv_flat_report
     ) gmv_cost
         inner join ods_fd_vb.ods_fd_region r
                    on lower(r.region_name) = lower(gmv_cost.country) and r.parent_id = 0 and r.region_display = 1
where ads_site_code in ("FD", "AD")
  and date >= "2020-01-01"
and platform != "others"
group by date, if(ads_site_code = 'FD', 'floryday', 'airydress'), ga_channel, r.region_code, platform
with cube
having event_date != "all";

drop table tmp.tmp_dwb_fd_ad_est;
create table tmp.tmp_dwb_fd_ad_est as
select
/*+ REPARTITION(1) */
    nvl(install_date, 'all') AS event_date,
    nvl(project_name, 'all') AS project_name,
    nvl(ga_channel, 'all')   AS ga_channel,
    nvl(country, 'all')      AS country_code,
    nvl(platform, 'all')     AS platform_type,
    sum(est_gmv_7d)          AS est_gmv_7d
from (
         select install_date,
                project_name,
                ga_channel,
                r.region_code as country,
                case
                    when ga_channel regexp 'ios' then 'ios_app'
                    when ga_channel regexp 'android' then 'android_app'
                    else 'others' end as platform,
                est_gmv_7d
         from ods_fd_ar.ods_fd_temp_device_order_date_cohort odc
         left join ods_fd_vb.ods_fd_region r on r.region_display = 1 and r.region_type = 0 and lower(r.region_name) = lower(odc.country)
         where install_date >= '2020-01-01'
           and project_name in ('floryday', 'airydress')
     ) est
group by cube (install_date, project_name, ga_channel, country, platform)
having event_date!="all"
;

CREATE TABLE IF NOT EXISTS tmp.dwb_fd_ads_cost
(
    event_date       date COMMENT 'event_date',
    project_name     string COMMENT 'datasource',
    country_code     string COMMENT 'region_code',
    ga_channel       string COMMENT 'ga_channel',
    platform         string COMMENT 'platform',
    activate_dau     bigint COMMENT '当天激活用户数',
    tot_gmv          decimal(20, 2) COMMENT 'gmv',
    tot_cost         decimal(20, 2) COMMENT '当天广告花费',
    tot_country_gmv  decimal(20, 2) COMMENT '分国家不分渠道platform 的gmv',
    tot_country_cost decimal(20, 2) COMMENT '分国家不分渠道platform 的cost',
    gmv_7d           decimal(20, 2) COMMENT '210天前-180天前时间段30天每天近7天激活用户gmv',
    gmv_180d         decimal(20, 2) COMMENT '210天前-180天前时间段30天每天近180天激活用户gmv',
    est_gmv_7d       decimal(20, 2) COMMENT '预估广告花费'
) COMMENT 'dwb_fd_ads_cost' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE;


with order_channel as (
    select oi.project_name,
           ai.ga_channel,
           oi.country_code,
           oi.platform_type,
           oi.goods_amount as gmv,
           from_utc_timestamp(ai.install_time,"PRC") as activate_time_prc,
           from_utc_timestamp(from_unixtime(oi.pay_time),"PRC")        as pay_time_prc
    from dwd.dwd_fd_order_info oi
             inner join dwd.dwd_fd_app_install ai on oi.device_id = ai.device_id and oi.project_name = ai.project_name
    where oi.project_name in ("floryday", "airydress")
      and oi.platform_type in ("ios_app", "android_app")
      and date(from_unixtime(pay_time)) >= "2020-01-01"
      and date(ai.install_time) between "2020-01-01" and "2021-04-01"
      and date(ai.install_time) <= date(from_unixtime(pay_time))

    union all

    select oi.project_name,
           ai.ga_channel,
           oi.country_code,
           oi.platform_type,
           oi.goods_amount as gmv,
           from_utc_timestamp(ai.install_time,"PRC") as activate_time_prc,
           from_utc_timestamp(from_unixtime(oi.pay_time),"PRC")        as pay_time_prc
    from dwd.dwd_fd_order_info oi
             inner join dwd.dwd_fd_app_install ai on oi.device_id != ai.device_id and oi.project_name = ai.project_name and oi.idfv = lower(ai.idfv) and ai.idfv != "" and ai.idfv is not null
    where oi.project_name in ("floryday", "airydress")
      and oi.platform_type in ("ios_app", "android_app")
      and date(from_unixtime(pay_time)) >= "2020-01-01"
      and date(ai.install_time) between "2020-01-01" and "2021-04-01"
      and date(ai.install_time) <= date(from_unixtime(pay_time))
),
     active_base as (
         select nvl(date(from_utc_timestamp(install_time,"PRC")), "all") as active_date,
                nvl(project_name, "all")        as project_name,
                nvl(country_code, "all")        as country_code,
                nvl(ga_channel, "all")          as ga_channel,
                nvl(platform_type, "all")       as platform_type,
                count(distinct device_id)       as activate_dau
         from dwd.dwd_fd_app_install
         group by date(from_utc_timestamp(install_time,"PRC")), project_name,
                  country_code, ga_channel, platform_type
         with cube
         having active_date != "all"
     ),
     date_table as (
         select i as index, date_add(start_date, i) as date
         from (
                  select "2020-01-01"     as start_date,
                         `current_date`() as end_date
              ) tmp
                  lateral view posexplode(split(space(datediff(end_date, start_date)), " ")) pe as i, x
     ),
     gmv_30_180d as (select nvl(dt.date, "all")          as date,
                            nvl(oc.project_name, "all")  as project_name,
                            nvl(oc.ga_channel, "all")    as ga_channel,
                            nvl(oc.country_code, "all")  as country_code,
                            nvl(oc.platform_type, "all") as platform_type,
                            sum(gmv)                     as gmv
                     from date_table dt
                              left join order_channel oc on datediff(oc.pay_time_prc, oc.activate_time_prc) < 180
                         and dt.date > date_add(activate_time_prc, 180)
                         and dt.date <= date_add(activate_time_prc, 210)
                     group by dt.date, oc.project_name, oc.ga_channel, oc.country_code, oc.platform_type
                     with cube
                     having date != "all"
     ),
     gmv_30_7d as (select nvl(dt.date, "all")          as date,
                          nvl(oc.project_name, "all")  as project_name,
                          nvl(oc.ga_channel, "all")    as ga_channel,
                          nvl(oc.country_code, "all")  as country_code,
                          nvl(oc.platform_type, "all") as platform_type,
                          sum(gmv)                     as gmv
                   from date_table dt
                            left join order_channel oc on datediff(oc.pay_time_prc, oc.activate_time_prc) < 7
                       and dt.date > date_add(activate_time_prc, 180)
                       and dt.date <= date_add(activate_time_prc, 210)
                   group by dt.date, oc.project_name, oc.ga_channel, oc.country_code, oc.platform_type
                   with cube
                   having date != "all"
     )
insert
overwrite
table
tmp.dwb_fd_ads_cost
partition
(
pt
)
select
       /*+ REPARTITION(1) */
       cost_gmv_base.event_date,
       cost_gmv_base.project_name,
       cost_gmv_base.country_code,
       cost_gmv_base.ga_channel,
       cost_gmv_base.platform_type,
       nvl(active_base.activate_dau, 0) as activate_dau,
       nvl(cost_gmv_base.tot_gmv, 0)    as tot_gmv,
       nvl(cost_gmv_base.tot_cost, 0)   as tot_cost,
       nvl(cost_gmv_base2.tot_gmv, 0)   as tot_country_gmv,
       nvl(cost_gmv_base2.tot_cost, 0)  as tot_country_cost,
       nvl(gmv_30_7d.gmv, 0)            as gmv_7d,
       nvl(gmv_30_180d.gmv, 0)          as gmv_180d,
       nvl(est.est_gmv_7d, 0)           as est_gmv_7d,
       cost_gmv_base.event_date

from tmp.tmp_dwb_fd_ad_cost_gmv cost_gmv_base
         left join active_base
                   on cost_gmv_base.event_date = active_base.active_date
                       and cost_gmv_base.project_name = active_base.project_name
                       and cost_gmv_base.platform_type = active_base.platform_type
                       and cost_gmv_base.country_code = active_base.country_code
                       and cost_gmv_base.ga_channel = active_base.ga_channel
         left join gmv_30_180d
                   on cost_gmv_base.event_date = gmv_30_180d.date
                       and cost_gmv_base.project_name = gmv_30_180d.project_name
                       and cost_gmv_base.platform_type = gmv_30_180d.platform_type
                       and cost_gmv_base.country_code = gmv_30_180d.country_code
                       and cost_gmv_base.ga_channel = gmv_30_180d.ga_channel
         left join gmv_30_7d
                   on cost_gmv_base.event_date = gmv_30_7d.date
                       and cost_gmv_base.project_name = gmv_30_7d.project_name
                       and cost_gmv_base.platform_type = gmv_30_7d.platform_type
                       and cost_gmv_base.country_code = gmv_30_7d.country_code
                       and cost_gmv_base.ga_channel = gmv_30_7d.ga_channel
         left join tmp.tmp_dwb_fd_ad_est est
                   on cost_gmv_base.event_date = est.event_date
                       and cost_gmv_base.project_name = est.project_name
                       and cost_gmv_base.platform_type = est.platform_type
                       and cost_gmv_base.country_code = est.country_code
                       and cost_gmv_base.ga_channel = est.ga_channel
         left join tmp.tmp_dwb_fd_ad_cost_gmv cost_gmv_base2
                   on cost_gmv_base.event_date = cost_gmv_base2.event_date
                       and cost_gmv_base.project_name = cost_gmv_base2.project_name
                       and cost_gmv_base.country_code = cost_gmv_base2.country_code
                       and cost_gmv_base2.platform_type = 'all'
                       and cost_gmv_base2.ga_channel = 'all'
                       and cost_gmv_base.ga_channel in ('all','facebook_api_android');