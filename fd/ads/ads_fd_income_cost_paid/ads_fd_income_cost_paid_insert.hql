insert overwrite table ads.ads_fd_income_cost_paid partition (pt='${pt}')
select /*+ REPARTITION(1) */t.project
     , t.country_code
     ,CASE
        WHEN t.country_code in ('All')  THEN  'All'
        WHEN t.country_code in ('others') THEN  'others'
      ELSE r.region_name end as country_name
     , t.pt_date
     , nvl(t.purchase_cost, 0.0) as purchase_cost
     , nvl(t.sale_amount, 0.0)   as sale_amount
     , nvl(t.coupon_cost, 0.0)   as coupon_cost
     , nvl(t.ads_cost, 0.0)      as ads_cost
     , nvl(t.refund_cost, 0.0)   as refund_cost
     , nvl(t.total_cost, 0.0)    as total_cost
from (
         select nvl(t2.project, 'All')                                          as project
              , nvl(t2.country_code, 'All')                                      as country_code
              , nvl(t2.pt, '0000-00-00')                                         as pt_date
              , sum(t2.sale_amount)                                              as sale_amount
              , sum(t2.coupon_cost)                                              as coupon_cost
              , sum(t2.ads_cost)                                                 as ads_cost
              , sum(t2.purchase_cost)                                            as purchase_cost
              , sum(t2.refund_cost)                                              as refund_cost
              , sum(t2.total_cost)                                               as total_cost
         from (
                  select to_date(order_time)                                          as pt
                       , nvl(lower(project), 'others')                                as project
                       , if(country_code in
                            ('DE', 'FR', 'GB', 'ES', 'IT', 'SA', 'RU', 'SE', 'BR', 'NO', 'NL', 'MX', 'CH', 'DK', 'AT',
                             'AU', 'PL', 'BE', 'ZA', 'CZ'), country_code, 'others')  as country_code
                       , nvl(goods_amount, 0.0)                                      as sale_amount
                       , nvl(bouns_amount, 0.0)                                      as coupon_cost
                       , nvl(ads_cost, 0.0)                                          as ads_cost
                       , nvl(es_purchase_amount, 0.0)                                as purchase_cost
                       , nvl(if(goods_refund_amount != 0,goods_refund_amount,order_amount_diff), 0.0)   as refund_cost
                       , nvl(bouns_amount, 0.0) + nvl(ads_cost, 0.0) + nvl(es_purchase_amount, 0.0) + nvl(if(goods_refund_amount != 0,goods_refund_amount,order_amount_diff), 0.0) as total_cost
                  from dwd.dwd_fd_ecs_order_info_paid
          ) t2
         group by t2.project, t2.pt, t2.country_code with cube
) t
left join (
    select
        region_code,region_name
    from ods_fd_ecshop.ods_fd_ecs_region
    where region_type = 0
) r ON upper(t.country_code) = upper(r.region_code)
where t.pt_date != '0000-00-00' and t.project in('floryday','airydress');