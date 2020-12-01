insert overwrite table dwd.dwd_fd_snowplow_order partition (pt = '${pt}')
select
    /*+ REPARTITION(1) */
    if(t2.domain_userid is not null,t2.project_name,t3.project_name) as project_name,
    if(t2.domain_userid is not null,t2.country,t3.country) as country,
    if(t2.domain_userid is not null,t2.platform_type,t3.platform_type) as platform_type,
    if(t2.domain_userid is not null,t2.page_code,t3.page_code) as page_code,
    if(t2.domain_userid is not null,t2.list_type,t3.list_type) as list_type,
    t1.user_id,
    t1.domain_userid as domain_userid,
    t1.order_id,
    t1.pay_status,
    cast(t1.gmv as decimal(10,2)) as order_amount
from(

    SELECT
     ogi.user_id,
     ud.sp_duid as domain_userid,
     ogi.order_id,
     '9' as pay_status,
     null as gmv
    FROM (

        select user_id,order_id
         from ods_fd_vb.ods_fd_order_info
         where  date(to_utc_timestamp(order_time, 'America/Los_Angeles')) = '${pt}'
         and email NOT REGEXP "tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com"
         group by user_id,order_id

    ) ogi
    LEFT JOIN (
        SELECT oud.user_id,oud.sp_duid
        FROM (
         SELECT
             user_id,
             sp_duid,
             created_time,
             to_utc_timestamp(created_time, 'America/Los_Angeles') as create_time_utc,
             last_update_time,
             row_number() over (partition by user_id order by last_update_time desc) as rn
         FROM ods_fd_vb.ods_fd_user_duid
         WHERE  sp_duid is not null
        ) oud where oud.rn = 1

    ) ud ON CAST(ogi.user_id AS string) = CAST(ud.user_id AS string) where ud.sp_duid is not null

    union all
    SELECT
         tab1.user_id,
         tab1.sp_duid as domain_userid,
         tab1.order_id,
         cast(tab1.pay_status as string) as pay_status,
         sum(tab1.goods_amount + tab1.shipping_fee) as gmv
    FROM (
        SELECT
         ogi.user_id,
         ud.sp_duid,
         ogi.order_id,
         ogi.pay_status,
         ogi.goods_amount,
         ogi.shipping_fee
        FROM (
            select user_id,order_id,goods_amount,shipping_fee,pay_status
            from ods_fd_vb.ods_fd_order_info
            where (
                date(to_utc_timestamp(pay_time, 'America/Los_Angeles')) = '${pt}'
                or date(to_utc_timestamp(order_time, 'America/Los_Angeles')) = '${pt}'
            )
            and pay_status = 2
            and email NOT REGEXP "tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com"

        ) ogi
        LEFT JOIN (
            SELECT oud.user_id,oud.sp_duid
            FROM (
             SELECT
                 user_id,
                 sp_duid,
                 created_time,
                 to_utc_timestamp(created_time, 'America/Los_Angeles') as create_time_utc,
                 last_update_time,
                 row_number() over (partition by user_id order by last_update_time desc) as rn
             FROM  ods_fd_vb.ods_fd_user_duid
             WHERE sp_duid is not null
            ) oud where oud.rn = 1

        ) ud ON cast(ogi.user_id as string) = cast(ud.user_id as string) where ud.sp_duid is not null

    ) tab1 group by tab1.user_id,tab1.sp_duid,tab1.order_id,tab1.pay_status

)t1
left join (
    SELECT t0.project_name,t0.country,t0.platform_type,t0.page_code,t0.list_type,t0.domain_userid
    from (
        SELECT
            project AS project_name,
            upper(country) as country,
            platform_type,
            page_code,
            goods_event_struct.list_type,
            domain_userid,
            row_number() over (partition by domain_userid order by derived_tstamp desc) as rn
        FROM ods_fd_snowplow.ods_fd_snowplow_goods_event
        WHERE pt >= date_sub('${pt}',20) and pt <= '${pt}'
        AND event_name = 'goods_click'
        AND project is not null
        AND project != ''
        AND length(country) = 2
        AND platform_type is not null
        AND platform_type != ''
        AND page_code != '404'
        AND page_code != ''
        AND page_code != 'afterPay'
        AND goods_event_struct.list_type is not null
        AND goods_event_struct.list_type != 'null'
        AND goods_event_struct.list_type != 'NULL'
        AND goods_event_struct.list_type != ''

    ) t0 where t0.rn = 1
)t2 on t1.domain_userid = t2.domain_userid
left join (
    SELECT t0.project_name,t0.country,t0.platform_type,t0.page_code,t0.list_type,t0.domain_userid
    from (
        SELECT
            project AS project_name,
            upper(country) as country,
            platform_type,
            page_code,
            goods_event_struct.list_type,
            domain_userid,
            row_number() over (partition by domain_userid order by derived_tstamp desc) as rn
        FROM ods_fd_snowplow.ods_fd_snowplow_goods_event
        WHERE pt >= date_sub('${pt}',20) and pt <= '${pt}'
        AND event_name = 'goods_impression'
        AND project is not null
        AND project != ''
        AND length(country) = 2
        AND platform_type is not null
        AND platform_type != ''
        AND page_code != '404'
        AND page_code != ''
        AND page_code != 'afterPay'
        AND goods_event_struct.list_type is not null
        AND goods_event_struct.list_type != 'null'
        AND goods_event_struct.list_type != 'NULL'
        AND goods_event_struct.list_type != ''
    ) t0 where t0.rn = 1
)t3 on t1.domain_userid = t3.domain_userid
where length(if(t2.domain_userid is not null,t2.project_name,t3.project_name)) > 0 ;
