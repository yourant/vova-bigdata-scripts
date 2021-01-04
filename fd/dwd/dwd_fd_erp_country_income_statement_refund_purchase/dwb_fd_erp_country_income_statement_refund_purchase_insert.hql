INSERT OVERWRITE TABLE dwd.dwd_fd_erp_country_income_statement_refund_purchase PARTITION (pt = '${pt}')
SELECT data_type
     , order_id
     , purchase_cost
     , refund_cost
FROM (
    /* 组织+国家 采购花费 */
     SELECT 'purchase'                                                                         AS data_type
          , t.order_id
          , sum(if(t.is_batch = 0, coalesce(t.unit_cost / t.currency_conversion_rate, 0.00),
                   coalesce(t.unit_cost / t.currency_conversion_rate * t.goods_number, 0.00))) AS purchase_cost
          , 0.00                                                                               AS refund_cost
     FROM (
              SELECT p.order_id
                   , p.unit_cost
                   , p.goods_number
                   , p.is_batch
                   , coalesce(c.currency_conversion_rate, 1.0)                                  AS currency_conversion_rate
                   , rank() OVER (PARTITION BY p.inventory_item_id ORDER BY c.currency_conversion_ts DESC) AS rn
              FROM (
                       SELECT oird.order_id
                            , oird.goods_number
                            , ii.unit_cost
                            , ii.inventory_item_id
                            , ii.currency
                            , ii.created_stamp
                            , eg.is_batch
                       FROM (
                                SELECT order_id
                                      , goods_number
                                      , order_inv_reserved_detail_id
                                      , reserved_time
                                 FROM ods_fd_romeo.ods_fd_order_inv_reserved_detail
                                 WHERE date(to_utc_timestamp(reserved_time, "Asia/Shanghai")) = '${pt}'
                            ) oird
                                LEFT JOIN
                            (
                                SELECT order_inv_reserved_detail_id
                                     , inventory_item_id
                                FROM ods_fd_romeo.ods_fd_order_inv_reserverd_inventory_mapping o
                                group by order_inv_reserved_detail_id, inventory_item_id
                            ) oirim ON oird.order_inv_reserved_detail_id = oirim.order_inv_reserved_detail_id
                                LEFT JOIN
                            (
                                SELECT o.unit_cost
                                     , o.inventory_item_id
                                     , o.currency
                                     , o.created_stamp
                                     , o.product_id
                                FROM ods_fd_romeo.ods_fd_inventory_item o
                            ) ii ON oirim.inventory_item_id = ii.inventory_item_id
                                LEFT JOIN
                            (
                                select product_id, is_batch
                                from ods_fd_ecshop.ods_fd_ecs_goods
                            ) eg ON eg.product_id = ii.product_id
                   ) p
                       LEFT JOIN
                   (
                       SELECT currency_conversion_rate
                            , to_currency_code
                            , currency_conversion_date
                            ,if(cast(currency_conversion_date as string) != '0000-00-00 00:00:00' and cast(currency_conversion_date as string) != '', cast(unix_timestamp(to_utc_timestamp(currency_conversion_date, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss") as bigint), 0) as currency_conversion_ts
                       FROM ods_fd_romeo.ods_fd_currency_conversion
                       WHERE from_currency_code = 'USD'
                         AND currency_conversion_date IS NOT NULL
                         AND cancellation_flag != 'Y'
                   ) c ON (p.currency = c.to_currency_code AND p.created_stamp >= c.currency_conversion_date)
     ) t WHERE t.rn = 1 GROUP BY t.order_id

     UNION ALL
    /* 组织+国家 退款花费-订单维度 */
     SELECT 'refund'                   AS data_type
          , t.order_id
          , 0.00                       AS purchase_cost
          , (coalesce(t.total_amount, 0.00) - coalesce(t.shipping_amount, 0.00)) /
            t.currency_conversion_rate AS refund_cost
     FROM (
              SELECT rr.order_id
                   , rr.total_amount
                   , rr.shipping_amount
                   , coalesce(c.currency_conversion_rate, 1.0)                                        as currency_conversion_rate
                   , rank() OVER (PARTITION BY rr.refund_id ORDER BY c.currency_conversion_ts DESC) AS rn
              FROM (

                    SELECT o.order_id
                         , o.refund_id
                         , o.currency
                         , o.created_stamp
                         , o.total_amount
                         , o.shipping_amount
                         , o.execute_date
                    FROM ods_fd_romeo.ods_fd_refund o
                    WHERE `status` = 'RFND_STTS_EXECUTED'
                      AND execute_date IS NOT NULL
                      AND to_date(to_utc_timestamp(execute_date, "Asia/Shanghai")) = '${pt}'

                   ) rr
                       LEFT JOIN
                   (
                       SELECT currency_conversion_rate
                            , to_currency_code
                            , currency_conversion_date
                            ,if(cast(currency_conversion_date as string) != '0000-00-00 00:00:00' and cast(currency_conversion_date as string) != '', cast(unix_timestamp(to_utc_timestamp(currency_conversion_date, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss") as bigint), 0) as currency_conversion_ts
                       FROM ods_fd_romeo.ods_fd_currency_conversion
                       WHERE from_currency_code = 'USD'
                         AND currency_conversion_date IS NOT NULL
                         AND cancellation_flag != 'Y'
                   ) c ON (rr.currency = c.to_currency_code AND rr.created_stamp >= c.currency_conversion_date)
     ) t WHERE t.rn = 1

    /* 组织+国家 退款花费-发货维度 */
     UNION ALL
     select 'refund_shipping'                            AS data_type
          , t.order_id
          , 0.00                                         AS purchase_cost
          , t.refund_amount / t.currency_conversion_rate as refund_cost
     from (
              select rr.order_id,
                     (coalesce(rr.total_amount, 0.00) - coalesce(rr.shipping_amount, 0.00))           as refund_amount,
                     coalesce(currency_conversion_rate, 1.0)                                          as currency_conversion_rate,
                     rank() OVER (PARTITION BY rr.refund_id ORDER BY c.currency_conversion_ts DESC) AS rn
              from (
                       SELECT   o.order_id
                                , o.refund_id
                                , o.currency
                                , unix_timestamp(o.created_stamp,'yyyy-MM-dd HH:mm:ss')  as created_stamp
                                , o.total_amount
                                , o.shipping_amount
                                , o.execute_date
                           FROM ods_fd_romeo.ods_fd_refund o
                           WHERE `status` = 'RFND_STTS_EXECUTED'
                             AND execute_date IS NOT NULL
                             AND to_date(to_utc_timestamp(execute_date, "Asia/Shanghai")) = '${pt}'
                   ) rr
                       inner join
                   (
                       select order_id, shipping_time
                       from ods_fd_ecshop.ods_fd_ecs_order_info
                       where shipping_status = 1
                         AND order_type_id = 'SALE'
                         AND email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
                   ) eoi on rr.order_id = eoi.order_id and rr.created_stamp >= eoi.shipping_time
                       LEFT JOIN
                   (
                       SELECT currency_conversion_rate
                            , to_currency_code
                            , unix_timestamp(currency_conversion_date,'yyyy-MM-dd HH:mm:ss') as currency_conversion_date
                            ,if(cast(currency_conversion_date as string) != '0000-00-00 00:00:00' and cast(currency_conversion_date as string) != '', cast(unix_timestamp(to_utc_timestamp(currency_conversion_date, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss") as bigint), 0) as currency_conversion_ts
                       FROM ods_fd_romeo.ods_fd_currency_conversion
                       WHERE from_currency_code = 'USD'
                         AND currency_conversion_date IS NOT NULL
                         AND cancellation_flag != 'Y'
                   ) c ON (rr.currency = c.to_currency_code AND rr.created_stamp >= c.currency_conversion_date)
     ) t where t.rn = 1

    /* 组织+国家 退款花费-订单金额和实际收款差值 */
     UNION ALL
     select 'refund_diff' AS data_type
          , t.order_id
          , 0.00 AS purchase_cost
          , (t.order_amount_exchange - t.order_amount) / t.currency_conversion_rate as refund_cost
     from (
              select eoi.order_id,
                     coalesce(oi.order_amount_exchange, 0.0) as order_amount_exchange,
                     coalesce(eoi.order_amount, 0.0) as order_amount,
                     coalesce(currency_conversion_rate, 1.0) as currency_conversion_rate,
                     rank() OVER (PARTITION BY eoi.order_id ORDER BY c.currency_conversion_ts DESC) AS rn
              from (
                      select order_id,
                             taobao_order_sn,
                             order_time,
                             currency,
                             order_amount
                      from ods_fd_ecshop.ods_fd_ecs_order_info
                      where date(to_utc_timestamp(order_time, "Asia/Shanghai")) = '${pt}'
                        AND pay_status = 2
                        AND order_type_id = 'SALE'
                        AND email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
              ) eoi
              inner join (
                      select order_sn,
                             order_amount_exchange
                      FROM dwd.dwd_fd_order_info
                      where  date(from_unixtime(order_time,'yyyy-MM-dd HH:mm:ss')) = '${pt}'
                        AND  pay_status = 2

              ) oi on eoi.taobao_order_sn = oi.order_sn
              LEFT JOIN (
                   SELECT currency_conversion_rate
                        , to_currency_code
                        , currency_conversion_date
                        , if(cast(currency_conversion_date as string) != '0000-00-00 00:00:00' and cast(currency_conversion_date as string) != '', cast(unix_timestamp(to_utc_timestamp(currency_conversion_date, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss") as bigint), 0) as currency_conversion_ts
                   FROM ods_fd_romeo.ods_fd_currency_conversion
                   WHERE from_currency_code = 'USD'
                     AND currency_conversion_date IS NOT NULL
                     AND cancellation_flag != 'Y'
               ) c ON (eoi.currency = c.to_currency_code AND eoi.order_time >= c.currency_conversion_date)
     ) t  where t.rn = 1
) t;