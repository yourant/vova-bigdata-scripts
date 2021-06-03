insert overwrite table tmp.ysj_20210601
select pt,
       nvl(platform, 'all')    as platform,
       nvl(region_code, 'all') as region_code,
       sum(new_coupon_cnt)     as new_coupon_cnt,
       sum(expre_uv)           as expre_uv,
       sum(expre_pv)           as expre_pv,
       sum(click_uv)           as click_uv,
       sum(click_pv)           as click_pv,
       sum(order_uv)           as order_uv,
       sum(pay_uv)             as pay_uv,
       sum(sales_number_002)   as sales_number_002,
       sum(gmv_002)            as gmv_002,
       sum(sales_number)       as sales_number,
       sum(gmv)                as gmv,
       sum(expre_uv_all)       as expre_uv_all,
       sum(expre_pv_all)       as expre_pv_all,
       sum(click_pv_all)       as click_pv_all
from (
         select cur_date as    pt,
                platform,
                region_code,
                new_coupon_cnt new_coupon_cnt,
                0        as    expre_uv,
                0        as    expre_pv,
                0        as    click_uv,
                0        as    click_pv,
                0        as    order_uv,
                0        as    pay_uv,
                0        as    sales_number_002,
                0        as    gmv_002,
                0        as    sales_number,
                0        as    gmv,
                0        as    expre_uv_all,
                0        as    expre_pv_all,
                0        as    click_pv_all
         from (
                  select to_date(activate_time)    as cur_date,
                         dd.platform,
                         dd.region_code,
                         count(distinct device_id) as new_coupon_cnt
                  from dim.dim_vova_devices dd
                           left join dim.dim_vova_coupon dc
                                     on dc.buyer_id = dd.current_buyer_id
                                         and dd.datasource = dc.datasource
                  where to_date(activate_time) >= '2021-05-01'
                    and to_date(activate_time) <= to_date(current_date())
                    and dc.cpn_cfg_id = 1730387
                    and dd.region_code in ('DE', 'ES', 'IT', 'FR', 'GB')
                    and dd.datasource = 'vova'
                  group by to_date(activate_time),
                           dd.platform,
                           dd.region_code
                  with cube
              ) tmp
         where cur_date is not null
         union all
         select pt,
                os_type  as platform,
                country  as region_code,
                0        as new_coupon_cnt,
                expre_uv as expre_uv,
                expre_pv as expre_pv,
                0        as click_uv,
                0        as click_pv,
                0        as order_uv,
                0        as pay_uv,
                0        as sales_number_002,
                0        as gmv_002,
                0        as sales_number,
                0        as gmv,
                0        as expre_uv_all,
                0        as expre_pv_all,
                0        as click_pv_all
         from (
                  select a.pt,
                         a.os_type,
                         a.country,
                         count(distinct a.device_id) as expre_uv,
                         count(*)                    as expre_pv
                  from dwd.dwd_vova_log_goods_impression a
                           inner join (
                      select distinct to_date(activate_time) as activate_time, device_id
                      from dim.dim_vova_devices dd
                               inner join dim.dim_vova_coupon dc
                                          on dc.buyer_id = dd.current_buyer_id
                                              and dc.cpn_cfg_id = 1730387
                                              and dd.datasource = dc.datasource
                      where dd.datasource = 'vova'
                        and dd.region_code in ('DE', 'ES', 'IT', 'FR', 'GB')
                  ) dd
                                      on to_date(dd.activate_time) = a.pt
                                          and dd.device_id = a.device_id
                           left join dim.dim_vova_goods dg
                                     on dg.virtual_goods_id = a.virtual_goods_id
                           inner join (
                      select distinct pt, goods_id
                      from ads.ads_vova_activity_user_only
                  ) new
                                      on new.pt = a.pt
                                          and new.goods_id = dg.goods_id
                  where a.os_type IN ('ios', 'android')
                    and a.country in ('DE', 'ES', 'IT', 'FR', 'GB')
                    and a.pt >= '2021-05-01'
                    and a.pt <= to_date(current_date())
                    and a.dp = 'vova'
                  group by a.pt,
                           a.os_type,
                           a.country
                  with cube
              ) tmp
         where pt is not null
         union all
         select pt,
                os_type  as platform,
                country  as region_code,
                0        as new_coupon_cnt,
                0        as expre_uv,
                0        as expre_pv,
                click_uv as click_uv,
                click_pv as click_pv,
                0        as order_uv,
                0        as pay_uv,
                0        as sales_number_002,
                0        as gmv_002,
                0        as sales_number,
                0        as gmv,
                0        as expre_uv_all,
                0        as expre_pv_all,
                0        as click_pv_all
         from (
                  select a.pt,
                         a.os_type,
                         a.country,
                         count(distinct a.device_id) as click_uv,
                         count(*)                    as click_pv
                  from dwd.dwd_vova_log_goods_click a
                           inner join (
                      select distinct to_date(activate_time) as activate_time, device_id
                      from dim.dim_vova_devices dd
                               inner join dim.dim_vova_coupon dc
                                          on dc.buyer_id = dd.current_buyer_id
                                              and dc.cpn_cfg_id = 1730387
                                              and dd.datasource = dc.datasource
                      where dd.datasource = 'vova'
                        and dd.region_code in ('DE', 'ES', 'IT', 'FR', 'GB')
                  ) dd
                                      on to_date(dd.activate_time) = a.pt
                                          and dd.device_id = a.device_id
                           left join dim.dim_vova_goods dg
                                     on dg.virtual_goods_id = a.virtual_goods_id
                           inner join (
                      select distinct pt, goods_id
                      from ads.ads_vova_activity_user_only
                  ) new
                                      on new.pt = a.pt
                                          and new.goods_id = dg.goods_id
                  where a.os_type IN ('ios', 'android')
                    and a.country in ('DE', 'ES', 'IT', 'FR', 'GB')
                    and a.pt >= '2021-05-01'
                    and a.pt <= to_date(current_date())
                    and a.dp = 'vova'
                  group by a.pt,
                           a.os_type,
                           a.country
                  with cube
              ) tt
         where pt is not null
         union all
         select pt,
                platform    as platform,
                region_code as region_code,
                0           as new_coupon_cnt,
                0           as expre_uv,
                0           as expre_pv,
                0           as click_uv,
                0           as click_pv,
                order_uv    as order_uv,
                0           as pay_uv,
                0           as sales_number_002,
                0           as gmv_002,
                0           as sales_number,
                0           as gmv,
                0           as expre_uv_all,
                0           as expre_pv_all,
                0           as click_pv_all
         from (
                  select to_date(dog.order_time)       as pt,
                         dog.platform,
                         dog.region_code,
                         count(distinct dog.device_id) as order_uv
                  from dim.dim_vova_order_goods dog
                           inner join (
                      select distinct to_date(activate_time) as activate_time, device_id
                      from dim.dim_vova_devices dd
                               inner join dim.dim_vova_coupon dc
                                          on dc.buyer_id = dd.current_buyer_id
                                              and dc.cpn_cfg_id = 1730387
                                              and dd.datasource = dc.datasource
                      where dd.datasource = 'vova'
                        and dd.region_code in ('DE', 'ES', 'IT', 'FR', 'GB')
                  ) dd
                                      on to_date(dd.activate_time) = to_date(dog.order_time)
                                          and dd.device_id = dog.device_id
                           inner join (
                      select distinct pt, goods_id
                      from ads.ads_vova_activity_user_only
                  ) new
                                      on new.pt = to_date(dog.order_time)
                                          and new.goods_id = dog.goods_id
                  where to_date(dog.order_time) >= '2021-05-01'
                    and to_date(dog.order_time) <= to_date(current_date())
                    and dog.platform IN ('ios', 'android')
                    and dog.region_code in ('DE', 'ES', 'IT', 'FR', 'GB')
                  group by to_date(dog.order_time),
                           dog.platform,
                           dog.region_code
                  with cube
              )
         where pt is not null
         union all
         select pt,
                platform    as platform,
                region_code as region_code,
                0           as new_coupon_cnt,
                0           as expre_uv,
                0           as expre_pv,
                0           as click_uv,
                0           as click_pv,
                0           as order_uv,
                pay_uv      as pay_uv,
                sales_number_002,
                gmv_002,
                0           as sales_number,
                0           as gmv,
                0           as expre_uv_all,
                0           as expre_pv_all,
                0           as click_pv_all
         from (
                  select to_date(dog.pay_time)                         as pt,
                         dog.platform,
                         dog.region_code,
                         count(distinct dog.device_id)                 as pay_uv,
                         sum(dog.goods_number)                         as sales_number_002,
                         sum(goods_number * shop_price + shipping_fee) as gmv_002
                  from dim.dim_vova_order_goods dog
                           inner join (
                      select distinct to_date(activate_time) as activate_time, device_id
                      from dim.dim_vova_devices dd
                               inner join dim.dim_vova_coupon dc
                                          on dc.buyer_id = dd.current_buyer_id
                                              and dc.cpn_cfg_id = 1730387
                                              and dd.datasource = dc.datasource
                      where dd.datasource = 'vova'
                        and dd.region_code in ('DE', 'ES', 'IT', 'FR', 'GB')
                  ) dd
                                      on to_date(dd.activate_time) = to_date(dog.pay_time)
                                          and dd.device_id = dog.device_id
                           inner join (
                      select distinct pt, goods_id
                      from ads.ads_vova_activity_user_only
                  ) new
                                      on new.pt = to_date(dog.pay_time)
                                          and new.goods_id = dog.goods_id
                  where to_date(dog.pay_time) >= '2021-05-01'
                    and to_date(dog.pay_time) <= to_date(current_date())
                    and dog.platform IN ('ios', 'android')
                    and dog.region_code in ('DE', 'ES', 'IT', 'FR', 'GB')
                  group by to_date(dog.pay_time),
                           dog.platform,
                           dog.region_code
                  with cube
              )
         where pt is not null
         union all
         select pt,
                platform    as platform,
                region_code as region_code,
                0           as new_coupon_cnt,
                0           as expre_uv,
                0           as expre_pv,
                0           as click_uv,
                0           as click_pv,
                0           as order_uv,
                0           as pay_uv,
                0           as sales_number_002,
                0           as gmv_002,
                sales_number,
                gmv,
                0           as expre_uv_all,
                0           as expre_pv_all,
                0           as click_pv_all
         from (
                  select to_date(dog.pay_time)                         as pt,
                         dog.platform,
                         dog.region_code,
                         sum(dog.goods_number)                         as sales_number,
                         sum(goods_number * shop_price + shipping_fee) as gmv
                  from dim.dim_vova_order_goods dog
                           inner join (
                      select distinct to_date(activate_time) as activate_time, device_id
                      from dim.dim_vova_devices dd
                      where dd.datasource = 'vova'
                        and dd.region_code in ('DE', 'ES', 'IT', 'FR', 'GB')
                  ) dd
                                      on to_date(dd.activate_time) = to_date(dog.pay_time)
                                          and dd.device_id = dog.device_id
                  where to_date(dog.pay_time) >= '2021-05-01'
                    and to_date(dog.pay_time) <= to_date(current_date())
                    and dog.platform IN ('ios', 'android')
                    and dog.region_code in ('DE', 'ES', 'IT', 'FR', 'GB')
                  group by to_date(dog.pay_time),
                           dog.platform,
                           dog.region_code
                  with cube
              )
         where pt is not null
         union all
         select pt,
                os_type  as platform,
                country  as region_code,
                0        as new_coupon_cnt,
                0        as expre_uv,
                0        as expre_pv,
                0        as click_uv,
                0        as click_pv,
                0        as order_uv,
                0        as pay_uv,
                0        as sales_number_002,
                0        as gmv_002,
                0        as sales_number,
                0        as gmv,
                expre_uv as expre_uv_all,
                expre_pv as expre_pv_all,
                0        as click_pv_all
         from (
                  select a.pt,
                         a.os_type,
                         a.country,
                         count(distinct a.device_id) as expre_uv,
                         count(*)                    as expre_pv
                  from dwd.dwd_vova_log_goods_impression a
                           inner join (
                      select distinct to_date(activate_time) as activate_time, device_id
                      from dim.dim_vova_devices dd
                      where dd.datasource = 'vova'
                        and dd.region_code in ('DE', 'ES', 'IT', 'FR', 'GB')
                  ) dd
                                      on to_date(dd.activate_time) = a.pt
                                          and dd.device_id = a.device_id
                  where a.os_type IN ('ios', 'android')
                    and a.country in ('DE', 'ES', 'IT', 'FR', 'GB')
                    and a.pt >= '2021-05-01'
                    and a.pt <= to_date(current_date())
                    and a.dp = 'vova'
                  group by a.pt,
                           a.os_type,
                           a.country
                  with cube
              ) tmp
         where pt is not null
         union all
         select pt,
                os_type  as platform,
                country  as region_code,
                0        as new_coupon_cnt,
                0        as expre_uv,
                0        as expre_pv,
                0        as click_uv,
                0        as click_pv,
                0        as order_uv,
                0        as pay_uv,
                0        as sales_number_002,
                0        as gmv_002,
                0        as sales_number,
                0        as gmv,
                0        as expre_uv_all,
                0        as expre_pv_all,
                click_pv as click_pv_all
         from (
                  select a.pt,
                         a.os_type,
                         a.country,
                         count(*) as click_pv
                  from dwd.dwd_vova_log_goods_click a
                           inner join (
                      select distinct to_date(activate_time) as activate_time, device_id
                      from dim.dim_vova_devices dd
                      where dd.datasource = 'vova'
                        and dd.region_code in ('DE', 'ES', 'IT', 'FR', 'GB')
                  ) dd
                                      on to_date(dd.activate_time) = a.pt
                                          and dd.device_id = a.device_id
                  where a.os_type IN ('ios', 'android')
                    and a.country in ('DE', 'ES', 'IT', 'FR', 'GB')
                    and a.pt >= '2021-05-01'
                    and a.pt <= to_date(current_date())
                    and a.dp = 'vova'
                  group by a.pt,
                           a.os_type,
                           a.country
                  with cube
              ) tmp
         where pt is not null
     ) tt
group by pt,
         platform,
         region_code
order by 1, 2, 3