with usd_currency_conversion as (
    SELECT currency_conversion_rate
         , to_currency_code
         , currency_conversion_date                                    as currency_conversion_shanghai_ts
         , to_utc_timestamp(currency_conversion_date, "Asia/Shanghai") as currency_conversion_utc_ts
    FROM ods_fd_romeo.ods_fd_currency_conversion
    WHERE from_currency_code = 'USD'
      AND currency_conversion_date IS NOT NULL
      AND cancellation_flag != 'Y'
)

insert overwrite table dwd.dwd_fd_refund_executed
select
    /*+ REPARTITION(5) */
    refund_id                                                                  as refund_id,
    order_id                                                                   as ecs_order_id,
    refund_currency.party_id                                                   as party_id,
    lower(p.name)                                                              as project,
    nvl(total_amount / usd_currency_conversion_rate, 0.00)                     as total_refund_amount,
    nvl((total_amount - shipping_amount) / usd_currency_conversion_rate, 0.00) as goods_refund_amount,
    nvl(shipping_amount / usd_currency_conversion_rate, 0.00)                  as shipping_refund_amount,
    to_utc_timestamp(execute_date, 'Asia/Shanghai')                            as execute_time
from (
   select r.refund_id,
          r.order_id,
          r.party_id,

          r.total_amount,
          r.shipping_amount,
          r.execute_date,
          nvl(ucc.currency_conversion_rate, 1.0)                                                    as usd_currency_conversion_rate,
          rank() OVER (PARTITION BY r.refund_id ORDER BY ucc.currency_conversion_shanghai_ts DESC) AS currency_rn
   from ods_fd_romeo.ods_fd_refund r
   left join usd_currency_conversion ucc on r.currency = ucc.to_currency_code and r.created_stamp >= ucc.currency_conversion_shanghai_ts
   where status = 'RFND_STTS_EXECUTED'
) refund_currency
left join ods_fd_romeo.ods_fd_party p on p.party_id = refund_currency.party_id
where currency_rn = 1;