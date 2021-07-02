INSERT OVERWRITE table dwb.dwb_fd_order_detail
select
    /*+ REPARTITION(30) */
    dg.order_id,
    from_unixtime(dg.order_time,'yyyy-MM-dd HH:mm:ss') as order_time_utc, 
    from_unixtime(dg.pay_time,'yyyy-MM-dd HH:mm:ss') as pay_time_utc,
    case
        when lower(fdpsc.ga_channel) = 'others' then 'others'
        when length(fdpsc.ga_channel) > 1 and lower(fdpsc.ga_channel) != 'others' then fdpsc.ga_channel
        else 'others'
    end as ga_channel,
    if((oi.pay_note = 'BOOK_ORDER'), 1, 0) as is_pre_order,
    dg.bonus,
    dg.project_name,
    dg.country_code,
    dg.pay_status,
    dg.virtual_goods_id,
    dg.shop_price*dg.goods_number as goods_amount,
    dg.goods_number,
    dg.cat_id,
    dg.goods_id,
    dg.cat_name,
    dg.platform_type
from
    dwd.dwd_fd_order_goods dg

left join
    ods_fd_vb.ods_fd_order_info oi
on
    dg.order_id = oi.order_id

left join
(
    select
        order_id,
        sp_session_id
        from ods_fd_vb.ods_fd_order_marketing_data
    group by
        order_id,
        sp_session_id
)om
on
    dg.order_id = om.order_id

left join
(
    select
        session_id,
        collect_set(ga_channel)[0] as ga_channel
    from
        dwd.dwd_fd_session_channel
    group by
        session_id
)fdpsc
on
    fdpsc.session_id = om.sp_session_id
where
    from_unixtime(dg.order_time,'yyyy-MM-dd') >= '2020-01-01';
