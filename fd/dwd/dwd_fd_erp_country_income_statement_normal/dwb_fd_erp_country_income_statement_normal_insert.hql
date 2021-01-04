INSERT OVERWRITE TABLE dwd.dwd_fd_erp_country_income_statement_normal PARTITION (pt='${pt}')
SELECT
    order_id,
    party_name,
    country_code,
    country_name,
    sales_amount,
    coupon_cost,
    ads_cost
FROM (
/* 组织+国家 订单销售额和订单红包花费 */
    SELECT
        oi.order_id
        ,lower(woi.project_name) as party_name
        ,if(r.region_code is null or r.region_code = '', 'others', r.region_code) AS country_code
        ,if(r.region_name is null or r.region_name = '', 'others', r.region_name) AS country_name
        ,coalesce(woi.goods_amount_exchange / currency_conversion_rate, 0.00) AS sales_amount
        ,coalesce(oi.bonus / currency_conversion_rate * -1, 0.00) AS coupon_cost
        ,0.00 AS ads_cost
    FROM (
        SELECT
            order_id
            ,taobao_order_sn
            ,country AS country_id
            ,party_id
            ,bonus
            ,coalesce(currency_conversion_rate, 1.0) as currency_conversion_rate
            ,rank() OVER (PARTITION BY eoi.order_id ORDER BY c.currency_conversion_ts DESC) AS rn
        FROM ods_fd_ecshop.ods_fd_ecs_order_info eoi
        LEFT JOIN (
            SELECT
                   currency_conversion_rate
                   ,to_currency_code
                   ,currency_conversion_date
                   ,if(cast(currency_conversion_date as string) != '0000-00-00 00:00:00' and cast(currency_conversion_date as string) != '', cast(unix_timestamp(to_utc_timestamp(currency_conversion_date, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss") as bigint), 0) as currency_conversion_ts
            FROM ods_fd_romeo.ods_fd_currency_conversion
            WHERE from_currency_code = 'USD'
            AND currency_conversion_date IS NOT NULL
            AND cancellation_flag != 'Y'
        ) c ON (eoi.currency = c.to_currency_code AND eoi.order_time >= c.currency_conversion_date)
        WHERE date(to_utc_timestamp(eoi.order_time, "Asia/Shanghai")) = '${pt}'
          AND eoi.pay_status=2
          AND eoi.order_type_id = 'SALE'
          AND eoi.email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
    ) oi
    INNER JOIN  ods_fd_ecshop.ods_fd_ecs_region r ON oi.country_id = r.region_id AND r.region_type = 0
    INNER JOIN (
        SELECT
            order_sn
            ,goods_amount_exchange
            ,project_name
        FROM dwd.dwd_fd_order_info
    ) woi ON oi.taobao_order_sn = woi.order_sn
    WHERE oi.rn = 1

    UNION ALL
/* 组织+国家 广告花费 */
    SELECT 0                                                                       AS order_id
         , adsr.party_name                                                         AS party_name
         , adsr.country_code                                                       AS country_code
         , adsr.country_name                                                       AS country_name
         , 0.00                                                                    AS sales_amount
         , 0.00                                                                    AS coupon_cost
         , sum(coalesce(adsr.ads_cost, 0.00))                                         AS ads_cost
    FROM (
            select
                    party_name
                    ,if(r.region_code is null or r.region_code = '', 'others', r.region_code) AS country_code
                    ,if(ads.country is null or ads.country  = '' and lower(ads.country) = 'others', 'others', ads.country) AS country_name
                    ,ads.ads_cost
            from(
                 SELECT CASE
                            WHEN ads_site_code = 'FD' THEN 'floryday'
                            WHEN ads_site_code = 'AD' THEN 'airydress'
                            WHEN ads_site_code = 'TD' THEN 'tendaisy'
                            WHEN ads_site_code = 'SD' THEN 'sisdress'
                         ELSE 'others' END as party_name
                         , country
                         , `cost` as ads_cost
                 FROM ods_fd_ar.ods_fd_ads_adgroup_daily_flat_report
                 WHERE `date` = '${pt}'
             )ads
             left JOIN ods_fd_ecshop.ods_fd_ecs_region r ON ads.country = r.region_name
     ) adsr
     GROUP BY adsr.party_name, adsr.country_code, adsr.country_name
) t
;