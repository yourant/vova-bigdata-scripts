USE dwd;

CREATE TABLE IF NOT EXISTS dwd.dwd_fd_user_window_buy (
    order_window INT COMMENT '订单下单时间窗口，年*12+月'
    ,email STRING COMMENT '用户邮箱地址'
    ,platform STRING COMMENT '用户访问平台类型（web or mob）'
    ,device_type STRING COMMENT '用户设备类型'
    ,window_sig STRING COMMENT '用户最早下单时间'
    ,order_amount DECIMAL(10, 2) COMMENT '订单总价格，商品总价+折扣+运费'
    ,country_code STRING COMMENT '国家缩写'
    ,project_name  STRING COMMENT '网站组织名'
) COMMENT '计算近半年每个用户每月购买习惯相关数据表'
PARTITIONED BY(
    dt STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS orc



INSERT OVERWRITE TABLE dwd.dwd_fd_user_window_buy PARTITION (dt='${hiveconf:dt}')
SELECT
    CAST(from_unixtime(oi.order_time, 'yyyy') * 12 + from_unixtime(oi.order_time, 'MM') AS INT) AS order_window
    ,u.email
    ,if(ua.is_app is null, 'other', if(ua.is_app = 0, 'web', 'mob')) AS platform
    ,if(ua.device_type is null, 'other', ua.device_type) AS device_type
    ,from_unixtime(min(oi.order_time), 'yyyy-MM-dd HH:mm:ss') AS window_sig
    ,SUM(CAST(oi.goods_amount + oi.bonus + oi.shipping_fee AS DECIMAL(10, 2))) AS order_amount
    ,r.region_code AS country_code
    ,oi.project_name
FROM (
    SELECT
        order_time
        ,user_id
        ,user_agent_id
        ,goods_amount
        ,bonus
        ,shipping_fee
        ,project_name
        ,country_id
    FROM dwd.dwd_fd_order_info
    WHERE
        dt='${hiveconf:dt}'
        and  pay_status = 1
        and order_time is not null
        and date(from_unixtime(order_time,'yyyy-MM-dd hh:mm:ss')) <= '${hiveconf:dt}' 
) oi
INNER JOIN (
    SELECT
        user_id
        ,email
    FROM ods_fd_vb.ods_fd_users
    WHERE
        email NOT LIKE '%@tetx.com%'
) u ON oi.user_id = u.user_id
LEFT JOIN ods_fd_vb.ods_fd_region r ON (oi.country_id = r.region_id)
LEFT JOIN ods_fd_vb.ods_fd_user_agent_analysis ua ON (oi.user_agent_id = ua.user_agent_id)
GROUP BY
    CAST(from_unixtime(oi.order_time, 'yyyy') * 12 + from_unixtime(oi.order_time, 'MM') AS INT)
    ,u.email
    ,if(ua.is_app is null, 'other', if(ua.is_app = 0, 'web', 'mob'))
    ,if(ua.device_type is null, 'other', ua.device_type)
    ,r.region_code
    ,oi.project_name;
