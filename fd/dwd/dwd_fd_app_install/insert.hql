set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

insert overwrite table dwd.dwd_fd_app_install partition (pt)
select
    /*+ REPARTTITION(1) */
    lower(device_id),
    lower(project_name),
    case
        when upper(country_code) = "UK" then "GB"
        else upper(country_code)
        end as country_code,
    case platform
        when 'ios' then 'ios_app'
        when 'android' then 'android_app'
        else 'other'
        end as platform_type
        ,
    install_time,
    lower(media_source),
    lower(campaign),
    CASE
        WHEN lower(campaign) REGEXP ' app_dr| app dr' THEN 'app_dr_api'
        WHEN lower(campaign) REGEXP ' #app' THEN 'mweb'
        WHEN lower(media_source) REGEXP '2019_mweb' THEN 'mweb'
        WHEN lower(media_source) = 'aura_int' THEN 'aura_api_android'
        WHEN lower(media_source) = 'bytedanceglobal_int' AND platform = 'android'
            THEN 'bytedance_api_android'
        WHEN lower(media_source) = 'bytedanceglobal_int' AND platform = 'ios' THEN 'bytedance_api_ios'
        WHEN lower(media_source) = 'outbrain_int' AND platform = 'android' THEN 'outbrain_api_android'
        WHEN lower(media_source) = 'outbrain_int' AND platform = 'ios' THEN 'outbrain_api_ios'
        WHEN lower(media_source) = 'twitter' AND platform = 'android' THEN 'twitter_api_android'
        WHEN lower(media_source) = 'twitter' AND platform = 'ios' THEN 'twitter_api_ios'
        WHEN lower(media_source) = 'liftoff_int' AND platform = 'android' THEN 'liftoff_api_android'
        WHEN lower(media_source) = 'liftoff_int' AND platform = 'ios' THEN 'liftoff_api_ios'
        WHEN lower(media_source) = 'tapjoy_int' AND platform = 'android' THEN 'tapjoy_api_android'
        WHEN lower(media_source) = 'tapjoy_int' AND platform = 'ios' THEN 'tapjoy_api_ios'
        WHEN lower(media_source) = 'admitad1_int' AND platform = 'android' THEN 'admitad_api_android'
        WHEN lower(media_source) = 'admitad1_int' AND platform = 'ios' THEN 'admitad_api_ios'
        WHEN lower(media_source) = 'revcontent_int' AND platform = 'android' THEN 'revcontent_api_android'
        WHEN lower(media_source) = 'revcontent_int' AND platform = 'ios' THEN 'revcontent_api_ios'
        WHEN lower(media_source) = 'applovin_int' AND platform = 'android' THEN 'applovin_api_android'
        WHEN lower(media_source) = 'applovin_int' AND platform = 'ios' THEN 'applovin_api_ios'
        WHEN lower(media_source) = 'taboola_int' AND platform = 'android' THEN 'taboola_api_android'
        WHEN lower(media_source) = 'taboola_int' AND platform = 'ios' THEN 'taboola_api_ios'
        WHEN lower(media_source) = 'snapchat_int' AND platform = 'android' THEN 'snapchat_api_android'
        WHEN lower(media_source) = 'snapchat_int' AND platform = 'ios' THEN 'snapchat_api_ios'
        WHEN lower(media_source) = 'pinterest_int' AND platform = 'android' THEN 'pinterest_api_android'
        WHEN lower(media_source) = 'pinterest_int' AND platform = 'ios' THEN 'pinterest_api_ios'
        WHEN lower(media_source) = 'apple search ads' THEN 'apple_search_api'
        WHEN lower(media_source) REGEXP 'facebook ads' AND platform = 'android' THEN 'facebook_api_android'
        WHEN lower(media_source) REGEXP 'facebook ads' AND platform = 'ios' THEN 'facebook_api_ios'
        WHEN lower(media_source) REGEXP 'googleadwords_int' AND platform = 'android'
            THEN 'google_api_android'
        WHEN lower(media_source) REGEXP 'googleadwords_int' AND platform = 'ios' THEN 'google_api_ios'
        WHEN lower(media_source) REGEXP 'organic' AND platform = 'ios' THEN 'api_organic_ios'
        WHEN lower(media_source) REGEXP 'organic' AND platform = 'android' THEN 'api_organic_android'
        WHEN platform = 'ios' THEN 'api_other_ios'
        WHEN platform = 'android' THEN 'api_other_android'
        ELSE 'api_other'
        END AS ga_channel,
    idfa,
    idfv,
    advertising_id,
    imei,
    android_id,
    date(install_time)
from ods_fd_vb.ods_fd_appsflyer_record;