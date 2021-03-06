set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwb.dwb_fd_income_cost partition (pt)
select /*+ REPARTITION(1) */
       t.project
     , t.country_code
     , CASE
           WHEN t.country_code in ('All')  THEN  'All'
           WHEN t.country_code in ('others') THEN  'others'
       ELSE r.region_name end as country_name
     , t.dimension_type
     , t.pt_date

     , nvl(t.purchase_amount, 0.0) as purchase_amount
     , nvl(t.sale_amount, 0.0)   as sale_amount
     , nvl(t.coupon_cost, 0.0)   as coupon_cost
     , nvl(t.ads_cost, 0.0)      as ads_cost
     , nvl(t.refund_cost, 0.0)   as refund_cost
     , nvl(t.total_cost, 0.0)    as total_cost
     , t.pt_date as pt
from (
         select 'shipping'                                                       as dimension_type
              , nvl(t1.project, 'All')                                           as project
              , nvl(t1.country_code, 'All')                                      as country_code
              , nvl(t1.pt, '0000-00-00')                                         as pt_date
              , sum(t1.sale_amount)                                              as sale_amount
              , sum(t1.coupon_cost)                                              as coupon_cost
              , sum(t1.ads_cost)                                                 as ads_cost
              , sum(t1.purchase_amount)                                          as purchase_amount
              , sum(t1.refund_cost)                                              as refund_cost
              , sum(t1.total_cost)                                               as total_cost
         from (
                  select if(length(cast(shipping_time as STRING)) != 19, to_date(order_time), to_date(shipping_time)) as pt
                       , nvl(lower(project), 'others')                                        as project
                       , if(country_code in
                            ('DE', 'FR', 'GB', 'ES', 'IT', 'SA', 'RU', 'SE', 'BR', 'NO', 'NL', 'MX', 'CH', 'DK', 'AT',
                             'AU', 'PL', 'BE', 'ZA', 'CZ','US'), country_code, 'others')     as country_code
                       , nvl(goods_amount, 0.0)                                         as sale_amount
                       , nvl(bouns_amount, 0.0)                                         as coupon_cost
                       , nvl(ads_cost, 0.0)                                             as ads_cost
                       , nvl(purchase_amount, 0.0)                                      as purchase_amount
                       , nvl(goods_refund_amount, 0.0)                                  as refund_cost
                       , nvl(bouns_amount, 0.0) + nvl(ads_cost, 0.0) + nvl(purchase_amount, 0.0) + nvl(goods_refund_amount, 0.0) as total_cost
                  from dwd.dwd_fd_ecs_order_info_shipping
         ) t1
         group by t1.project, t1.pt, t1.country_code with cube

         union all

         select 'order'                                                          as dimension_type
              , nvl(t2.project, 'All')                                           as project
              , nvl(t2.country_code, 'All')                                      as country_code
              , nvl(t2.pt, '0000-00-00')                                         as pt
              , sum(t2.sale_amount)                                              as sale_amount
              , sum(t2.coupon_cost)                                              as coupon_cost
              , sum(t2.ads_cost)                                                 as ads_cost
              , sum(t2.purchase_amount)                                          as purchase_amount
              , sum(t2.refund_cost)                                              as refund_cost
              , sum(t2.total_cost)                                               as total_cost
         from (
                  select to_date(order_time) as pt
                       , nvl(lower(project), 'others')                                        as project
                       , if(country_code in
                            ('DE', 'FR', 'GB', 'ES', 'IT', 'SA', 'RU', 'SE', 'BR', 'NO', 'NL', 'MX', 'CH', 'DK', 'AT',
                             'AU', 'PL', 'BE', 'ZA', 'CZ','US'), country_code, 'others')    as country_code
                       , nvl(goods_amount, 0.0)                                        as sale_amount
                       , nvl(bouns_amount, 0.0)                                        as coupon_cost
                       , nvl(ads_cost, 0.0)                                            as ads_cost
                       , nvl(purchase_amount, 0.0)                                     as purchase_amount
                       , nvl(if(goods_refund_amount != 0,goods_refund_amount,order_amount_diff), 0.0) as refund_cost
                       , nvl(bouns_amount, 0.0) + nvl(ads_cost, 0.0) + nvl(purchase_amount, 0.0) + nvl(if(goods_refund_amount != 0,goods_refund_amount,order_amount_diff), 0.0) as total_cost
                  from dwd.dwd_fd_ecs_order_info_paid isn
         ) t2
         group by t2.project, t2.pt, t2.country_code with cube
) t
left join (
    select
        region_code,region_name
    from dim.dim_fd_ecs_region
    where region_type = 0
) r ON upper(t.country_code) = upper(r.region_code)
where t.pt_date != '0000-00-00' and t.pt_date <= '${pt}'  and t.project in('floryday','airydress');