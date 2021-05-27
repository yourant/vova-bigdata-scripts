set hive.strict.checks.cartesian.product=false;
set hive.mapred.mode=nonstrict;

with usd_currency_conversion as (
    SELECT currency_conversion_rate
         , to_currency_code
         , currency_conversion_date                                    as currency_conversion_shanghai_ts
         , to_utc_timestamp(currency_conversion_date, "Asia/Shanghai") as currency_conversion_utc_ts
    FROM ods_fd_romeo.ods_fd_currency_conversion
    WHERE from_currency_code = 'USD'
      AND currency_conversion_date IS NOT NULL
      AND cancellation_flag != 'Y'
),

ecs_order_info_with_currency as (
   select *
   from (
            select eoi.*,
                   nvl(ucc.currency_conversion_rate, 1.0)                                                     as usd_currency_conversion_rate,
                   ROW_NUMBER() OVER (PARTITION BY eoi.order_id ORDER BY ucc.currency_conversion_shanghai_ts DESC)  AS currency_rn
            from ods_fd_ecshop.ods_fd_ecs_order_info eoi
            left join usd_currency_conversion ucc on eoi.currency = ucc.to_currency_code AND eoi.order_time >= ucc.currency_conversion_shanghai_ts
        ) order_info_currency
   where currency_rn = 1
),

romeo_goods_purchase_with_currency as (
   select goods_id,
          price,
          currency_conversion_rate
   from (
            select gpp.goods_id,
                   gpp.price,
                   nvl(ucc.currency_conversion_rate, 1.0)                                                     as currency_conversion_rate,
                   ROW_NUMBER() OVER (PARTITION BY gpp.goods_id ORDER BY ucc.currency_conversion_shanghai_ts DESC) AS currency_rn
            from ods_fd_romeo.ods_fd_goods_purchase_price gpp
            left join usd_currency_conversion ucc on ucc.to_currency_code = 'RMB' and gpp.ctime >= ucc.currency_conversion_shanghai_ts
        ) goods_purchase_currency
   where currency_rn = 1
),

inventory_item_with_currency as (
   select *
   from (
            select ii.*,
                   nvl(ucc.currency_conversion_rate, 1.0)                                                             as usd_currency_conversion_rate,
                   ROW_NUMBER() OVER (PARTITION BY ii.inventory_item_id ORDER BY ucc.currency_conversion_shanghai_ts DESC) AS currency_rn
            from ods_fd_romeo.ods_fd_inventory_item ii
            left join usd_currency_conversion ucc on ii.currency = ucc.to_currency_code AND ii.created_stamp >= ucc.currency_conversion_shanghai_ts
    ) order_info_currency
   where currency_rn = 1
),

ecs_order_info_paid as (
  select order_info.ecs_order_id,
      order_info.order_sn,
      order_info.party_id,
      order_info.project,
      order_info.order_time,
      order_info.shipping_time,
      order_info.shipping_status,
      order_info.country_id,
      order_info.country_code,
      order_info.user_id,
      nvl(order_info.goods_amount, 0.00)                                                                as goods_amount,
      nvl(order_info.bouns_amount, 0.00)                                                                as bouns_amount,
      nvl(1 / order_info.pt_goods_cnt * ads_cost_table.pt_ads_cost,0.00)       as ads_cost,
      nvl(order_purchase_estimate.purchase_amount, 0.00)                                                as es_purchase_amount,
      nvl(order_purchase.purchase_amount, 0.00)                                                         as purchase_amount,
      nvl(order_refund.goods_refund_amount, 0.00)                                                       as goods_refund_amount,
      nvl(order_refund.shipping_refund_amount, 0.00)                                                    as shipping_refund_amount,
      nvl(order_info.ecs_order_amount, 0.00)                                                            as ecs_order_amount,
      nvl(voi.order_amount_exchange / usd_currency_conversion_rate, 0.00)                               as vb_order_amount,
      nvl(voi.order_amount_exchange / usd_currency_conversion_rate - order_info.ecs_order_amount, 0.00) as order_amount_diff
  from (

  -- ecs_order_info可以获取的数据
  -- 纬度以及goods_amount，bouns_amount
          select eoi.order_id                                                                            as ecs_order_id,
                 eoi.taobao_order_sn                                                                     as order_sn,
                 eoi.party_id,
                 lower(p.name)                                                                           as project,
                 date(to_utc_timestamp(eoi.order_time, "Asia/Shanghai"))                                 as pt,
                 sum(1)
                     over (partition by date(to_utc_timestamp(eoi.order_time, "Asia/Shanghai")),p.name,eoi.country) as pt_goods_cnt,
                 to_utc_timestamp(eoi.order_time, "Asia/Shanghai")                                       as order_time,
                 from_unixtime(eoi.shipping_time,'yyyy-MM-dd HH:mm:ss')                                  as shipping_time,
                 shipping_status                                                                         as shipping_status,
                 eoi.country                                                                             as country_id,
                 r.region_code                                                                           as country_code,
                 eoi.user_id                                                                             as user_id,
                 nvl(eoi.goods_amount / usd_currency_conversion_rate, 0.00)                              as goods_amount,
                 nvl(-1 * eoi.bonus / usd_currency_conversion_rate, 0.00)                                as bouns_amount,
                 nvl(eoi.order_amount / usd_currency_conversion_rate, 0.00)                              as ecs_order_amount,
                 usd_currency_conversion_rate                                                            as usd_currency_conversion_rate
         from ecs_order_info_with_currency eoi
         left join ods_fd_romeo.ods_fd_party p on p.party_id = eoi.party_id
         left join ods_fd_ecshop.ods_fd_ecs_region r on r.region_id = eoi.country and r.region_type = 0
             where eoi.pay_status = 2
               and eoi.order_type_id = 'SALE'
               and eoi.email not regexp '@tetx.com|@qq.com|@163.com|@vova.com.hk|@i9i8.com|@airydress.com'
  ) order_info

  -- ecs_order_goods获取采购价
  left join (
      select og.order_id                                                                    as ecs_order_id,
             sum(nvl(og.goods_number * rgpwc.price / rgpwc.currency_conversion_rate, 0.00)) as purchase_amount
      from ods_fd_ecshop.ods_fd_ecs_order_goods og
      left join ods_fd_ecshop.ods_fd_ecs_goods g on og.goods_id = g.goods_id
      left join romeo_goods_purchase_with_currency rgpwc on rgpwc.goods_id = g.external_goods_id
       group by og.order_id
  ) order_purchase_estimate on order_info.ecs_order_id = order_purchase_estimate.ecs_order_id

  -- 退款金额
  left join (
      select ecs_order_id                as ecs_order_id,
             sum(total_refund_amount)    as total_refund_amount,
             sum(shipping_refund_amount) as shipping_refund_amount,
             sum(goods_refund_amount)    as goods_refund_amount
      from dwd.dwd_fd_refund_executed
      where execute_time is not null
      group by ecs_order_id
  ) order_refund on order_info.ecs_order_id = order_refund.ecs_order_id

  -- 广告花费，按组织,国家 和pt聚合
  left join (
      select pt,
             project,
             country_code,
             sum(cost) as pt_ads_cost
      from (
          SELECT `date`               as pt,
                 if(r.region_code is null or r.region_code = '', 'others', r.region_code) AS country_code,
                 CASE
                     WHEN ads_site_code = 'FD' THEN 'floryday'
                     WHEN ads_site_code = 'AD' THEN 'airydress'
                     WHEN ads_site_code = 'TD' THEN 'tendaisy'
                     WHEN ads_site_code = 'SD' THEN 'sisdress'
                     ELSE 'others' END as project
                  ,
                 `cost`
          FROM ods_fd_ar.ods_fd_ads_adgroup_daily_flat_report ads
          left join ods_fd_ecshop.ods_fd_ecs_region r on r.region_name = ads.country and r.region_type = 0

      ) ads_cost
      group by pt, project,country_code
  ) ads_cost_table on order_info.project = ads_cost_table.project and order_info.country_code = ads_cost_table.country_code and order_info.pt = ads_cost_table.pt

  -- 真实采购花费，仅完全预定上的订单
  left join (
      select oird.order_id,
          sum(
              if(eg.is_batch = 0,
                 nvl(ii.unit_cost / ii.usd_currency_conversion_rate, 0.00),
                 nvl(ii.unit_cost / ii.usd_currency_conversion_rate * oird.goods_number, 0.00)
                )
          ) as purchase_amount
      from ods_fd_romeo.ods_fd_order_inv_reserved oir
      left join ods_fd_romeo.ods_fd_order_inv_reserved_detail oird
                on oir.order_inv_reserved_id = oird.order_inv_reserved_id
      left join ods_fd_romeo.ods_fd_order_inv_reserverd_inventory_mapping oirim
                on oird.order_inv_reserved_detail_id = oirim.order_inv_reserved_detail_id
      left join inventory_item_with_currency ii on ii.inventory_item_id = oirim.inventory_item_id
      left join ods_fd_ecshop.ods_fd_ecs_goods eg on eg.product_id = ii.product_id
      where oir.status in ('Y', 'F')
      group by oird.order_id
  ) order_purchase on order_info.ecs_order_id = order_purchase.order_id

  -- 网站订单
  left join ods_fd_vb.ods_fd_order_info voi on voi.order_sn = order_info.order_sn
)

from ecs_order_info_paid

insert overwrite table dwd.dwd_fd_ecs_order_info_paid
select ecs_order_id,
       order_sn,
       party_id,
       project,
       order_time,
       country_id,
       country_code,
       user_id,
       goods_amount,
       bouns_amount,
       ads_cost,
       es_purchase_amount,
       purchase_amount,
       goods_refund_amount,
       shipping_refund_amount,
       ecs_order_amount,
       vb_order_amount,
       order_amount_diff

insert overwrite table dwd.dwd_fd_ecs_order_info_shipping
select ecs_order_id,
       order_sn,
       party_id,
       project,
       order_time,
       shipping_time,
       country_id,
       country_code,
       user_id,
       goods_amount,
       bouns_amount,
       ads_cost,
       es_purchase_amount,
       purchase_amount,
       goods_refund_amount,
       shipping_refund_amount,
       ecs_order_amount,
       vb_order_amount,
       order_amount_diff
where shipping_status = 1
;