insert overwrite table dwd.dwd_fd_goods_click_detail partition (pt = '${pt3}')
select nvl(tab2.goods_id, 'UNKNOWN')         as goods_id,
       nvl(tab1.virtual_goods_id, 'UNKNOWN') as virtual_goods_id,
       nvl(tab1.project, 'UNKNOWN')          as project,
       nvl(tab1.country_code, 'UNKNOWN')     as country_code,
       nvl(tab1.platform_type, 'UNKNOWN')    as platform_type,
       tab1.add_session_id,
       tab1.goods_view_session_id,
       tab1.paid_order_id,
       tab1.goods_click_session_id,
       tab1.goods_impression_session_id,
       tab1.pt_date
from (
         select case
                    when fms.event_name IN ('goods_click', 'goods_impression') then single_goods_event.virtual_goods_id
                    when fms.event_name IN ('page_view', 'screen_view') and page_code = 'product'
                        then fms.url_virtual_goods_id
                    when fms.event_name IN ('add', 'checkout', 'remove') then single_ecommerce_event.id
                    end                                                                      as virtual_goods_id,
                lower(fms.project)                                                           as project,
                fms.country                                                                  as country_code,
                case
                    when fms.platform_type like '%_app' then 'mob'
                    when fms.platform_type = 'mobile_web' then 'h5'
                    when fms.platform_type = 'others' then 'others'
                    else 'web' end                                                           as platform_type,
                IF(fms.event_name = 'add', session_id, NULL)                                 as add_session_id,
                IF(fms.event_name = 'page_view' and page_code = 'product', session_id, NULL) as goods_view_session_id,
                null                                                                         as paid_order_id,
                IF(fms.event_name = 'goods_click', session_id, NULL)                         as goods_click_session_id,
                IF(fms.event_name = 'goods_impression', session_id, NULL)                    as goods_impression_session_id,
                cast(fms.pt as string)                                                       as pt_date
         from ods_fd_snowplow.ods_fd_snowplow_all_event fms
                  LATERAL VIEW OUTER explode(goods_event_struct) single_goods_event_table AS single_goods_event
                  LATERAL VIEW OUTER explode(ecommerce_product) single_ecommerce_event_table AS single_ecommerce_event
         where fms.pt >= date_sub('${pt3}', 3)
           and fms.pt <= date_add('${pt3}', 3)
           and fms.event_name in ('goods_click', 'goods_impression', 'page_view', 'screen_view', 'add')

         union all

         select cast(ogi.virtual_goods_id as string)   as virtual_goods_id,
                lower(ogi.project_name) as project,
                ogi.country_code        as country_code,
                case
                    when ogi.is_app = 0 and ogi.device_type in ('pc', 'pad') then 'web'
                    when ogi.is_app = 0 and ogi.device_type = 'mobile' then 'h5'
                    when ogi.is_app = 1 then 'mob'
                    else 'others' end   as platform_type,
                null                    as add_session_id,
                null                    as goods_view_session_id,
                ogi.order_id            as paid_order_id,
                null                    as goods_click_session_id,
                null                    as goods_impression_session_id,
                cast(date(from_unixtime(ogi.pay_time,'yyyy-MM-dd HH:mm:ss')) as string)  as pt_date
         from dwd.dwd_fd_order_goods ogi
         where date(from_unixtime(ogi.pay_time,'yyyy-MM-dd HH:mm:ss')) >= date_sub('${pt3}', 3)
           and date(from_unixtime(ogi.pay_time,'yyyy-MM-dd HH:mm:ss')) <= date_add('${pt3}', 3)
           and ogi.pay_status = 2
     ) tab1
    inner join (
        select
            cast(virtual_goods_id as string) as virtual_goods_id,
            goods_id
        from dwd.dwd_fd_order_goods_top
        where pt = '${pt3}' group by virtual_goods_id,goods_id
    ) tab2 on tab2.virtual_goods_id = tab1.virtual_goods_id
where tab1.pt_date != '${pt3}';