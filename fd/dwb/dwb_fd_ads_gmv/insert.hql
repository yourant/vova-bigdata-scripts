with order_channel as (
    select oi.project_name,
           ai.ga_channel,
           oi.country_code,
           oi.platform_type,
           oi.goods_amount as gmv,
           ai.install_time as install_time,
           from_unixtime(oi.pay_time)        as pay_time
    from dwd.dwd_fd_order_info oi
             inner join dwd.dwd_fd_app_install ai on oi.project_name = ai.project_name and oi.device_id = ai.device_id
    where oi.project_name in ("floryday", "airydress")
      and oi.platform_type in ("ios_app", "android_app")
      and date(from_unixtime(pay_time)) >= "2020-01-01"
      and date(ai.install_time) >= "2020-01-01"
      and date(ai.install_time) <= date(from_unixtime(pay_time))

    union all

    select oi.project_name,
           ai.ga_channel,
           oi.country_code,
           oi.platform_type,
           oi.goods_amount as gmv,
           ai.install_time as install_time,
           from_unixtime(oi.pay_time)        as pay_time
    from dwd.dwd_fd_order_info oi
             inner join dwd.dwd_fd_app_install ai on oi.project_name = ai.project_name and oi.device_id != ai.device_id and oi.device_id = lower(ai.idfv) and ai.idfv != "" and ai.idfv is not null
    where oi.project_name in ("floryday", "airydress")
      and oi.platform_type in ("ios_app", "android_app")
      and date(from_unixtime(pay_time)) >= "2020-01-01"
      and date(ai.install_time) >= "2020-01-01"
      and date(ai.install_time) <= date(from_unixtime(pay_time))
)

insert overwrite table tmp.dwb_fd_ads_gmv
select
/*+ REPARTITION(1) */
    nvl(trunc(date_format(from_utc_timestamp(oc.install_time,"PRC"), 'yyyy-MM-dd'), 'MM'), 'all')                             AS pt,
    nvl(oc.project_name, 'all')                                                                      AS project_name,
    nvl(oc.country_code, 'all')                                                                      AS country_code,
    nvl(oc.ga_channel, 'all')                                                                        AS ga_channel,
    nvl(oc.platform_type, 'all')                                                                     AS platform,
    sum(if((unix_timestamp(oc.pay_time) - unix_timestamp(oc.install_time)) / 3600 >= 0 AND
           (unix_timestamp(oc.pay_time) - unix_timestamp(oc.install_time)) / 3600 <= 24, gmv, 0))   AS gmv_1d,
    sum(if((unix_timestamp(oc.pay_time) - unix_timestamp(oc.install_time)) / 3600 >= 0 AND
           (unix_timestamp(oc.pay_time) - unix_timestamp(oc.install_time)) / 3600 <= 168, gmv, 0))  AS gmv_7d,
    sum(if((unix_timestamp(oc.pay_time) - unix_timestamp(oc.install_time)) / 3600 >= 0 AND
           (unix_timestamp(oc.pay_time) - unix_timestamp(oc.install_time)) / 3600 <= 720, gmv, 0))  AS gmv_30d,
    sum(if((unix_timestamp(oc.pay_time) - unix_timestamp(oc.install_time)) / 3600 >= 0 AND
           (unix_timestamp(oc.pay_time) - unix_timestamp(oc.install_time)) / 3600 <= 2160, gmv, 0)) AS gmv_90d,
    sum(if((unix_timestamp(oc.pay_time) - unix_timestamp(oc.install_time)) / 3600 >= 0 AND
           (unix_timestamp(oc.pay_time) - unix_timestamp(oc.install_time)) / 3600 <= 4320, gmv, 0)) AS gmv_180d

from order_channel oc
where (unix_timestamp(oc.pay_time) - unix_timestamp(oc.install_time)) / 3600 >= 0
  AND (unix_timestamp(oc.pay_time) - unix_timestamp(oc.install_time)) / 3600 <= 4320
group by trunc(date_format(from_utc_timestamp(oc.install_time,"PRC"), 'yyyy-MM-dd'), 'MM'), oc.project_name, oc.country_code,
         oc.ga_channel, oc.platform_type
with cube
HAVING pt != 'all';