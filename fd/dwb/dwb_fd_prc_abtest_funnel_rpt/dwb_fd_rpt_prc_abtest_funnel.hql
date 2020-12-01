

INSERT OVERWRITE TABLE dwb.dwb_fd_prc_abtest_funnel_rpt PARTITION (pt = '${pt}')

select
           nvl(project,'all'),
           nvl(platform_type,'all'),
           nvl(country,'all'),
           nvl(app_version,'all'),
           nvl(abtest_name,'all'),
           nvl(abtest_version,'all'),

           count(distinct session_id),
           count(distinct homepage_session_id),
           count(distinct list_session_id),
           count(distinct product_session_id),
           count(distinct cart_session_id),
           count(distinct add_session_id),
           count(distinct remove_session_id),
           count(distinct checkout_session_id),
           count(distinct checkout_option_session_id),
           count(distinct purchase_session_id),
           count(distinct checkout_page_session_id),
           count(distinct order_id),
           sum(goods_amount),
           sum(bonus),
           sum(shipping_fee)
from(
    select project,
           platform_type,
           country,
           app_version,
           substr(abtest_info, 1, instr(abtest_info, '=') - 1)  as abtest_name,
           substr(abtest_info, instr(abtest_info, '=') + 1)    as abtest_version,
           session_id,
           IF(page_code = 'homepage', session_id, null)           as homepage_session_id,
           IF(page_code in ('list', 'landing'), session_id, null) as list_session_id,
           IF(page_code = 'product', session_id, null)            as product_session_id,
           IF(page_code = 'cart', session_id, null)               as cart_session_id,
           IF(event_name = 'add', session_id, null)               as add_session_id,
           IF(event_name = 'remove', session_id, null)            as remove_session_id,
           IF(event_name = 'checkout', session_id, null)          as checkout_session_id,
           IF(event_name = 'checkout_option', session_id, null)   as checkout_option_session_id,
           IF(event_name = 'purchase', session_id, null)          as purchase_session_id,
           IF(((page_code = 'addressedit' and referrer_page_code in ('product', 'cart')) or (page_code = 'checkout')),
              session_id, null)                                   as checkout_page_session_id,
           NULL                                                 as order_id,
           0.0                                                  as goods_amount,
           0.0                                                  as bonus,
           0.0                                                  as shipping_fee
    from (
             select project,
                    platform_type,
                    event_name,
                    page_code,
                    referrer_page_code,
                    country,
                    app_version,
                    session_id,
                    abtest
             from ods_fd_snowplow.ods_fd_snowplow_all_event
             where event_name in ('page_view', 'screen_view', 'add', 'remove', 'checkout', 'checkout_option', 'purchase')
               and abtest != ''
               and abtest != '-'
               and (pt='${pt_last}' and hour between 16 and 24)
               or (pt='${pt}' and hour between 1 and 16)

         ) fms LATERAL VIEW OUTER explode(split(fms.abtest, '&')) fms as abtest_info

    union all

    select fboi.project_name                                   as project,
           fboi.platform_type                                  as platform_type,
           fboi.country_code                                   as country,
           fboi.version                                        as app_version,
           substr(abtest_info, 1, instr(abtest_info, '=') - 1) as abtest_name,
           substr(abtest_info, instr(abtest_info, '=') + 1)    as abtest_version,
           NULL                                                as session_id,
           NULL                                                as homepage_session_id,
           NULL                                                as list_session_id,
           NULL                                                as product_session_id,
           NULL                                                as cart_session_id,
           NULL                                                as add_session_id,
           NULL                                                as remove_session_id,
           NULL                                                as checkout_session_id,
           NULL                                                as checkout_option_session_id,
           NULL                                                as purchase_session_id,
           NULL                                                as checkout_page_session_id,
           cast(fboi.order_id as bigint),
           fboi.goods_amount,
           fboi.bonus,
           fboi.shipping_fee

    from (
        select oi.pay_time,
                oi.order_id,
                oi.project_name,
                oi.goods_amount,
                oi.bonus,
                oi.shipping_fee,
                oi.platform_type,
                oi.country_code,
                oi.version,
                oe.ext_value
        from (
            select
                order_id,
                project_name,
                goods_amount,
                pay_time,
                bonus,
                shipping_fee,
                platform_type,
                country_code,
                version
            from dwd.dwd_fd_order_info
            where date_format(from_utc_timestamp(from_unixtime(pay_time), 'PRC'), 'yyyy-MM-dd') = '${pt}'
            and pay_status = 2
            and email NOT REGEXP "tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com"
        )oi
        left join (select order_id,ext_value from ods_fd_vb.ods_fd_order_extension where ext_name = 'abtestInfo') oe on oi.order_id = oe.order_id

    ) fboi LATERAL VIEW OUTER explode(split(fboi.ext_value, '&')) fboi as abtest_info

)tab1
where    project is not null
        and  platform_type is not null
        and  country is not null
        and  app_version is not null
        and  abtest_name is not null
        and  abtest_version is not null


group by              project,
                      platform_type,
                      country,
                      app_version,
                      abtest_name,
                      abtest_version
                       with cube;
