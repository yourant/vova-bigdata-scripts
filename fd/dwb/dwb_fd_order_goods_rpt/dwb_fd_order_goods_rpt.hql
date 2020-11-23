CREATE table if not exists  dwb.dwb_fd_order_goods_rpt
(
        order_id bigint,
        user_id bigint,
        platform_type string,
        pay_status bigint,
        country_code string,
        party_id bigint,
        project_name string,
        order_amount decimal(15,4),
        shipping_fee decimal(15,4),
        goods_id bigint,
        cat_id bigint,
        cat_name string,
        goods_number bigint,
        shop_price decimal(15,4),
        virtual_goods_id bigint,
        email bigint,
        goods_amount double,
        order_time string,
        pay_time string
)comment '每日商品销售额/销量明细表'
partitioned by(`pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS orc
TBLPROPERTIES ("orc.compress"="SNAPPY");



insert overwrite table dwb.dwb_fd_order_goods_rpt partition(pt='{hiveconf:pt}')
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
where  (date(from_unixtime(order_time)='{hiveconf:pt}') or date(from_unixtime(pay_time))='{hiveconf:pt}') and pay_status = 2
and email NOT REGEXP "tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com";
