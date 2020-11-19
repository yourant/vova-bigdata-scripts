CREATE TABLE IF NOT EXISTS dwb.dwb_fd_prc_abtest_funnel_rpt
(
    project                    string,
    platform_type              string,
    country                    string,
    app_version                string,
    session_id                 string,
    homepage_session_id        string,
    list_session_id            string,
    product_session_id         string,
    cart_session_id            string,
    add_session_id             string,
    remove_session_id          string,
    checkout_session_id        string,
    checkout_option_session_id string,
    purchase_session_id        string,
    checkout_page_session_id   string,
    order_id                   bigint,
    goods_amount               DECIMAL(15, 4) comment '商品美元价格总和',
    bonus                      DECIMAL(15, 4) comment '订单折扣美元价格,负数',
    shipping_fee               DECIMAL(15, 4) comment '订单运费美元价格',
    abtest_name                string,
    abtest_version             string
)
    PARTITIONED BY ( pt string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS ORC
    TBLPROPERTIES ("orc.compress"="SNAPPY");

INSERT OVERWRITE TABLE dwb.dwb_fd_prc_abtest_funnel_rpt PARTITION (pt = '${hiveconf:pt}')
select project,
       platform_type,
       country,
       app_version,
       session_id,
       IF(page_code = 'homepage', session_id, '')           as homepage_session_id,
       IF(page_code in ('list', 'landing'), session_id, '') as list_session_id,
       IF(page_code = 'product', session_id, '')            as product_session_id,
       IF(page_code = 'cart', session_id, '')               as cart_session_id,
       IF(event_name = 'add', session_id, '')               as add_session_id,
       IF(event_name = 'remove', session_id, '')            as remove_session_id,
       IF(event_name = 'checkout', session_id, '')          as checkout_session_id,
       IF(event_name = 'checkout_option', session_id, '')   as checkout_option_session_id,
       IF(event_name = 'purchase', session_id, '')          as purchase_session_id,
       IF(((page_code = 'addressedit' and referrer_page_code in ('product', 'cart')) or (page_code = 'checkout')),
          session_id, '')                                   as checkout_page_session_id,
       NULL                                                 as order_id,
       0.0                                                  as goods_amount,
       0.0                                                  as bonus,
       0.0                                                  as shipping_fee,
       substr(abtest_info, 1, instr(abtest_info, '=') - 1)  as abtest_name,
       substr(abtest_info, instr(abtest_info, '=') + 1)     as abtest_version
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
         from ods.ods_fd_prc_snowplow_all_event
         where event_name in ('page_view', 'screen_view', 'add', 'remove', 'checkout', 'checkout_option', 'purchase')
           and abtest != ''
           and abtest != '-'
           and pt = '${hiveconf:pt}'
     ) fms LATERAL VIEW OUTER explode(split(fms.abtest, '&')) fms as abtest_info;

INSERT INTO TABLE dwb.dwb_fd_prc_abtest_funnel_report PARTITION (pt = '${hiveconf:pt}')
select fboi.project_name                                   as project,
       fboi.platform_type                                  as platform_type,
       fboi.country_code                                   as country,
       fboi.version                                        as app_version,
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
       fboi.shipping_fee,
       substr(abtest_info, 1, instr(abtest_info, '=') - 1) as abtest_name,
       substr(abtest_info, instr(abtest_info, '=') + 1)    as abtest_version
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
        where date_format(from_utc_timestamp(cast(pay_time * 1000 as timestamp), 'PRC'), 'yyyy-MM-dd') = '${hiveconf:pt}'
        and pay_status = 2
        and email NOT REGEXP "tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com"
    )oi
    left join (select order_id,ext_value from ods_fd_vb.ods_fd_order_extension where ext_name = 'abtestInfo') oe on oi.order_id = oe.order_id
    
) fboi LATERAL VIEW OUTER explode(split(fboi.ext_value, '&')) fboi as abtest_info;
