insert overwrite table dwd.dwd_fd_user_repurchase_weekly partition (pt = '${pt}')
SELECT
    /*+ REPARTITION(1) */
    tab1.first_day_week as current_week,
    tab1.user_id as user_id,
    tab1.order_id as order_id,
    tab1.project_name as project,
    tab1.country_code as country_code,
    tab1.platform_type as platform_type,
    tab1.ga_channel as ga_channel,
    if(voi.pay_at_first = tab1.first_day_week, 'yes', 'no') as user_is_first_pay,
    if(u.reg_at_first = tab1.first_day_week, 'yes', 'no') as user_is_first_reg
FROM(
    SELECT
        date_sub(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss'),cast(date_format(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss'),'u') as int)-1) as first_day_week,
        oi.user_id as user_id,
        oi.order_id as order_id,
        oi.project_name as project_name,
        if(upper(oi.country_code) in('FR', 'DE', 'SE', 'GB', 'AU', 'US', 'IT', 'ES', 'NL', 'MX', 'NO', 'AT', 'BE', 'CH', 'DK', 'CZ', 'PL', 'IL', 'BR', 'SA'),upper(oi.country_code),'others') as country_code,
        oi.platform_type,
        CASE
            WHEN oi.ga_channel in
            ('pla','sem','mobilepla','mobilesem','gdn','mobilegdn','google_api_android','google_api_ios') THEN 'google'
            WHEN oi.ga_channel in
            ('facebook_api_android','facebook_api_ios','facebook_dca','facebook') THEN 'facebook'
            WHEN oi.ga_channel in
            ('EDM') THEN 'EDM'
            WHEN oi.ga_channel in
            ('direct') THEN 'direct'
            WHEN oi.ga_channel in
            ('seo') THEN 'seo'
            ELSE 'others' end as ga_channel
    FROM dwd.dwd_fd_order_channel_analytics oi
    where date(from_unixtime(oi.pay_time,'yyyy-MM-dd HH:mm:ss')) >= date_sub('${pt}',63)
    and date(from_unixtime(oi.pay_time,'yyyy-MM-dd HH:mm:ss')) < '${pt}'
    and length(oi.project_name) > 0
    and oi.pay_status = 2
) tab1
LEFT JOIN
(
        SELECT  user_id,date_sub(pay_first,cast(date_format(pay_first,'u') as int)-1) as pay_at_first
        from(
            select user_id, date(min(from_unixtime(pay_time,'yyyy-MM-dd HH:mm:ss'))) as pay_first
            from dwd.dwd_fd_order_info
            where pay_status = 2
            group by user_id
        )tab1

)voi on tab1.user_id = voi.user_id
LEFT JOIN
(   
        SELECT user_id,date_sub(reg_first,cast(date_format(reg_first,'u') as int)-1) as reg_at_first
        from(
            select user_id, date(TO_UTC_TIMESTAMP(reg_time, 'America/Los_Angeles')) as reg_first
            from ods_fd_vb.ods_fd_users
            where email NOT REGEXP  '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
        )tab1
        
)u on tab1.user_id = u.user_id
;