CREATE TABLE IF NOT EXISTS dwb.dwb_fd_country_order_report
(
    project_name     string comment '组织',
    platform         string COMMENT '平台',
    country_code     string COMMENT '国家',
    pay_status       string COMMENT '支付状态',
    orders           bigint COMMENT '订单数',
    order_amount_free  DECIMAL(15, 4) COMMENT 'order_amount_free',
    order_amount       DECIMAL(15, 4) COMMENT 'order_amount',
    users              bigint COMMENT '用户数',
    customer_price_free   DECIMAL(15, 4) comment 'customer_price_free',
    customer_price      DECIMAL(15, 4) COMMENT 'customer_price',
    shipping_free       DECIMAL(15, 4) COMMENT 'shipping_free'
) COMMENT '国家订单相关金额报表'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

INSERT overwrite table dwb.dwb_fd_country_order_report PARTITION (dt = '${hiveconf:dt}')
select 
    nvl(tab1.project_name,'all') as project_name,
    nvl(tab1.platform,'all') as platform,
    nvl(tab1.country_code,'all') as country_code,
    nvl(tab1.pay_status,'all') as pay_status,
    nvl(COUNT(DISTINCT tab1.order_id),0) as orders, /*下单数*/
    nvl(SUM(tab1.goods_amount)+SUM(tab1.shipping_fee),0.0) as order_amount_free,
    nvl(SUM(tab1.goods_amount),0.0) as order_amount,
    nvl(COUNT(DISTINCT tab1.user_id),0.0) as users, /*用户数*/
    nvl(cast((SUM(tab1.goods_amount)+SUM(tab1.shipping_fee))/COUNT(DISTINCT tab1.user_id) as decimal(10,2)), 0.0) as customer_price_free,
    nvl(cast(SUM(tab1.goods_amount)/COUNT(DISTINCT tab1.user_id) as decimal(10,2)) ,0.0) as customer_price,
    nvl(SUM(tab1.shipping_fee),0.0) as shipping_free
from (

    select 
        to_date(from_utc_timestamp(from_unixtime(oi.event_date), 'UTC')) as event_dt,
        oi.order_id,
        oi.event_date,
        oi.user_id,
        if(oi.is_app is null, 'other', if(oi.is_app = 0, 'web', 'mob'))  AS platform,
        if(oi.device_type is null, 'other', oi.device_type)   AS device_type,
        oi.platform_type as platform_type,
        oi.order_time as order_time,
        cast(oi.pay_status as string) as pay_status,
        if(oi.pay_time < 0 or oi.pay_time is null, 0, oi.pay_time)  AS pay_time,
        oi.country_code as country_code,
        oi.language_code as language_code,
        oi.order_currency_code as order_currency_code,
        oi.project_name as project_name,
        oi.goods_amount as goods_amount,
        oi.goods_amount_exchange as goods_amount_exchange,
        oi.bonus as bonus,
        oi.shipping_fee as shipping_fee,
        oi.shipping_fee_exchange as shipping_fee_exchange,
        oi.email as email
    from dwd.dwd_fd_order_info oi 
    where dt = '${hiveconf:dt}'
    and (date(from_unixtime(order_time,'yyyy-MM-dd hh:mm:ss')) = '${hiveconf:dt}' or date(from_unixtime(pay_time,'yyyy-MM-dd hh:mm:ss')) = '${hiveconf:dt}')
    and oi.email NOT REGEXP "tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com"
)tab1
group by 
        tab1.project_name,
        tab1.platform,
        tab1.country_code,
        tab1.pay_status with cube;
