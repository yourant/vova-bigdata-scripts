insert overwrite table dwd.dwd_fd_goods_snowplow_performance partition (pt='${pt}')
select
       /*+ REPARTITION(5) */
       project,
       country,
       language,
       platform_type,
       platform_name,
       event_name,
       page_code,
       goods_event_name,
       g.goods_id,
       event.virtual_goods_id,
       session_id
from (
         select project,
                country,
                language,
                platform_type,
                case
                    when platform_type in ('pc_web', 'tablet_web') then 'PC'
                    when platform_type in ('mobile_web') then 'H5'
                    when platform_type in ('ios_app', 'android_app') then 'APP'
                    else 'Others'
                    end as platform_name,
                event_name,
                page_code,
                case
                    when event_name in ('page_view', 'screen_view') then "view"
                    when event_name = "goods_click" then "click"
                    when event_name = "goods_impression" then "impression"
                    else event_name
                    end as goods_event_name,
                session_id,
                case
                    when event_name in ('page_view', 'screen_view') then cast(url_virtual_goods_id as int)
                    when event_name in ("goods_click", "goods_impression")
                        then cast(goods_event.virtual_goods_id as int)
                    when event_name in ("add", "checkout") then cast(ecommerce_product_info.id as int)
                    else null
                    end as virtual_goods_id

         from ods_fd_snowplow.ods_fd_snowplow_all_event
                  LATERAL VIEW OUTER explode(goods_event_struct) goods_event_table as goods_event
                  LATERAL VIEW OUTER explode(ecommerce_product) ecommerce_product_table as ecommerce_product_info
         where pt = "${pt}"
             and event_name in ('page_view', 'screen_view') and page_code = "product" and
               url_virtual_goods_id is not null
            or event_name in ("goods_click", "goods_impression") and
               goods_event.list_type in ('list-category', 'list-pre-order')
            or event_name = "add" and page_code in ("product", "list")
            or event_name = "checkout") event
         left join dim.dim_fd_goods g on event.virtual_goods_id = g.virtual_goods_id