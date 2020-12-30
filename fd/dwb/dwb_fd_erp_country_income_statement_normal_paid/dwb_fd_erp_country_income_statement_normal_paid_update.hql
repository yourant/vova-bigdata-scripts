INSERT OVERWRITE TABLE dwb.dwb_fd_erp_country_income_statement_normal_paid PARTITION (pt='${pt}')
SELECT
    t.order_id
    ,t.party_name
    ,t.country_code
    ,t.country_name
    ,t.sales_amount
    ,t.coupon_cost
    ,t.ads_cost
    ,p.purchase_cost
FROM (
    SELECT
        m.order_id
        ,m.party_name
        ,m.country_code
        ,m.country_name
        ,m.sales_amount
        ,m.coupon_cost
        ,m.ads_cost
    FROM (
        SELECT
            order_id
            ,party_name
            ,country_code
            ,country_name
            ,sales_amount
            ,coupon_cost
            ,ads_cost
        FROM dwb.dwb_fd_erp_country_income_statement_normal
        WHERE pt = '${pt}'
          AND order_id IS NOT NULL
    ) m
) t
LEFT JOIN (
    SELECT
        eog.order_id
        ,sum(coalesce(eog.goods_number * gpp.price / gpp.currency_conversion_rate, 0.00)) AS purchase_cost
    FROM (
        SELECT
            n.order_id
            ,eg.external_goods_id as goods_id
            ,n.goods_number
        FROM  ods_fd_ecshop.ods_fd_ecs_order_goods n
        LEFT JOIN ods_fd_ecshop.ods_fd_ecs_goods eg ON (n.goods_id = eg.goods_id)
    ) eog
    LEFT JOIN (
        SELECT
            n.goods_id
            ,n.price
            ,coalesce(c.currency_conversion_rate, 1.0) as currency_conversion_rate
            ,rank() OVER (PARTITION BY n.goods_id ORDER BY c.currency_conversion_ts DESC) AS rn
        FROM ods_fd_romeo.ods_fd_goods_purchase_price n
        LEFT JOIN (
            SELECT
                   currency_conversion_rate
                   ,to_currency_code
                   ,unix_timestamp(currency_conversion_date, "yyyy-MM-dd HH:mm:ss") as currency_conversion_date
                   ,if(currency_conversion_date != '0000-00-00 00:00:00' and currency_conversion_date != '', unix_timestamp(to_utc_timestamp(currency_conversion_date, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) as currency_conversion_ts
            FROM fd.fd_base_romeo_currency_conversion
            WHERE from_currency_code = 'USD'
            AND currency_conversion_date IS NOT NULL
            AND currency_conversion_date != 0
            AND cancellation_flag != 'Y'
        ) c ON (c.to_currency_code = 'RMB' AND n.ctime >= c.currency_conversion_date)
    ) gpp ON (eog.goods_id = gpp.goods_id AND rn = 1)
    group by  eog.order_id
) p ON t.order_id = p.order_id ;