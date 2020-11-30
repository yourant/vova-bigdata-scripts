
insert overwrite table dwb.dwb_fd_order_goods_rpt partition(pt='${hiveconf:pt}')
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
where  date(from_unixtime(pay_time))= '${hiveconf:pt}'
and pay_status = 2
and email NOT REGEXP "tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com";
