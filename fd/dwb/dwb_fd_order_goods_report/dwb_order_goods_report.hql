create view if not exists dwb.dwb_fd_order_goods_report_view as 
select 
    order_id,
    user_id,
    platform_type,
    pay_status,
    country_code,
    party_id,
    project_name,
    order_amount,
    shipping_fee,
    goods_id,
    cat_id,
    cat_name,
    goods_number,
    shop_price,
    virtual_goods_id,
    email,
    goods_number*shop_price as goods_amount,
    date(from_unixtime(order_time)) as order_time,
    date(from_unixtime(pay_time)) as pay_time
from dwd.dwd_fd_order_goods
where  dt in (select max(dt)  from dwd.dwd_fd_order_goods) and pay_status = 2 
and email not like '%@tetx.com' 
and email not like '%@i9i8.com';
