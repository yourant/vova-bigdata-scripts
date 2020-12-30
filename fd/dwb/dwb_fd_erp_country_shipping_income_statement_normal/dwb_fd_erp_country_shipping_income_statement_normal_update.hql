INSERT OVERWRITE TABLE dwb.dwb_fd_erp_country_income_statement_normal PARTITION (pt='${pt}')
SELECT
    order_id
    ,party_name
    ,country_code
    ,country_name
    ,sales_amount
    ,coupon_cost
    ,ads_cost
FROM (
/* 组织+国家 订单销售额和订单红包花费 */
    SELECT
        oi.order_id
        ,lower(woi.project_name) as party_name
        ,r.region_code AS country_code
        ,r.region_name AS country_name
        ,coalesce(eoi.goods_amount / c.currency_conversion_rate, 0.00) AS sales_amount
        ,coalesce(eoi.bonus / c.currency_conversion_rate * -1, 0.00) AS coupon_cost
        ,coalesce(o.order_ads_cost, 0.00) AS ads_cost
    FROM (
        SELECT
            order_id
            ,country AS country_id
            ,party_id
            ,goods_amount
            ,bonus
            ,coalesce(currency_conversion_rate, 1.0) as currency_conversion_rate
            ,taobao_order_sn
            ,rank() OVER (PARTITION BY eoi.order_id ORDER BY c.currency_conversion_ts DESC) AS rn
        FROM ods_fd_ecshop.ods_fd_ecs_order_info eoi
        LEFT JOIN (
            SELECT
                   currency_conversion_rate
                   ,to_currency_code
                   ,currency_conversion_date
                   ,if(currency_conversion_date != '0000-00-00 00:00:00' and currency_conversion_date != '', unix_timestamp(to_utc_timestamp(currency_conversion_date, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) as currency_conversion_ts
            FROM ods_fd_romeo.ods_fd_currency_conversion
            WHERE from_currency_code = 'USD'
            AND currency_conversion_date IS NOT NULL
            AND currency_conversion_date != 0
            AND cancellation_flag != 'Y'
        ) c ON (eoi.currency = c.to_currency_code AND eoi.order_time >= c.currency_conversion_date)
        WHERE if(eoi.shipping_time = 0, date(to_utc_timestamp(eoi.order_time, "Asia/Shanghai")), from_unixtime(eoi.shipping_time,'yyyy-MM-dd')) = '${pt}'
          AND eoi.shipping_status=1
          AND eoi.order_type_id = 'SALE'
          AND eoi.email not regexp '@tetx.com|@i9i8.com'

    ) oi
    INNER JOIN ods_fd_ecshop.ods_fd_ecs_region r ON oi.country_id = r.region_id AND r.region_type = 0
    INNER JOIN (
        SELECT
            order_sn
            ,project_name
        FROM dwd.dwd_fd_order_info
    ) woi ON oi.taobao_order_sn = woi.order_sn
    LEFT JOIN (
        SELECT
            o.order_id
            ,coalesce(o.sales_amount / d.total_sales_amount * d.total_ads_cost, 0.0) as order_ads_cost
        FROM (
            SELECT
                order_id
                ,sales_amount
                ,party_name
                ,pt as order_pt
            FROM dwb.dwb_fd_erp_country_income_statement_normal
            WHERE pt between add_months('${pt}', -4) and date_add('${pt}', 1)
        ) o
        INNER JOIN (
            SELECT
                pt as order_pt
                ,party_name
                ,sum(coalesce(sales_amount, 0.0)) as total_sales_amount
                ,sum(coalesce(ads_cost, 0.0)) as total_ads_cost
            FROM dwb.dwb_fd_erp_country_income_statement_normal
            WHERE pt between add_months('${pt}', -4) and date_add('${pt}', 1)
            GROUP BY pt, party_name
        ) d ON (o.order_pt = d.order_pt and o.party_name = d.party_name)
    ) o ON (oi.order_id = o.order_id)
    WHERE oi.rn = 1
) t
;