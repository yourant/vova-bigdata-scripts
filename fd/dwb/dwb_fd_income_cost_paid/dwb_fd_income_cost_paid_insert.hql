insert overwrite table dwb.dwb_fd_income_cost_paid
select /*+ REPARTITION(1) */t.party_name
     , t.country_code
     , t.country_name
     , t.dt
     , nvl(t.purchase_cost, 0.0) as purchase_cost
     , nvl(t.sale_amount, 0.0)   as sale_amount
     , nvl(t.coupon_cost, 0.0)   as coupon_cost
     , nvl(t.ads_cost, 0.0)      as ads_cost
     , nvl(t.refund_cost, 0.0)   as refund_cost
     , nvl(t.total_cost, 0.0)    as total_cost
from (
         select nvl(t2.party_name, 'All')                                        as party_name
              , nvl(t2.country_code, 'All')                                      as country_code
              , if(t2.country_code is null, 'All', collect_set(country_name)[0]) as country_name
              , nvl(t2.dt, '0000-00-00')                                         as dt
              , sum(t2.sale_amount)                                              as sale_amount
              , sum(t2.coupon_cost)                                              as coupon_cost
              , sum(t2.ads_cost)                                                 as ads_cost
              , sum(t2.purchase_cost)                                            as purchase_cost
              , sum(t2.refund_cost)                                              as refund_cost
              , sum(t2.total_cost)                                               as total_cost
         from (
                  select dt
                       , nvl(party_name, 'Others')                                  as party_name
                       , if(country_code in
                            ('DE', 'FR', 'GB', 'ES', 'IT', 'SA', 'RU', 'SE', 'BR', 'NO', 'NL', 'MX', 'CH', 'DK', 'AT',
                             'AU', 'PL', 'BE', 'ZA', 'CZ'), country_code, 'others') as country_code
                       , if(country_code in
                            ('DE', 'FR', 'GB', 'ES', 'IT', 'SA', 'RU', 'SE', 'BR', 'NO', 'NL', 'MX', 'CH', 'DK', 'AT',
                             'AU', 'PL', 'BE', 'ZA', 'CZ'), country_name, 'others') as country_name
                       , nvl(sales_amount, 0.0)                                     as sale_amount
                       , nvl(coupon_cost, 0.0)                                      as coupon_cost
                       , nvl(ads_cost, 0.0)                                         as ads_cost
                       , nvl(purchase_cost, 0.0)                                    as purchase_cost
                       , nvl(refund_cost, 0.0)                                      as refund_cost
                       , nvl(coupon_cost, 0.0) + nvl(ads_cost, 0.0) + nvl(purchase_cost, 0.0) +
                         nvl(refund_cost, 0.0)                                      as total_cost
                  from FD.fd_mid_country_income_statement_normal_paid isn
                           LEFT JOIN(SELECT order_id,
                                            if(coalesce(sum(if(data_type = 'refund', refund_cost, 0.0)), 0.0) != 0, coalesce(sum(if(data_type = 'refund', refund_cost, 0.0)), 0.0), coalesce(sum(if(data_type = 'refund_diff', refund_cost, 0.0)), 0.0)) as refund_cost
                                     FROM FD.FD_MID_COUNTRY_INCOME_STATEMENT_REFUND_PURCHASE where data_type in ('purchase', 'refund', 'refund_diff')
                                     GROUP BY order_id) rp ON isn.order_id = rp.order_id
              ) t2
         group by t2.party_name, t2.dt, t2.country_code
         with cube
     ) t;