insert overwrite table tmp.fd_rpt_goods_details partition (dt='2020-12-07')

select  project_name,
       country_code,
       platform,
        goods_id,
        virtual_goods_id,
       cat_name,
       device_type,
       count(distinct impression_session_id) as goods_impression_session,
       count(distinct click_session_id) as goods_click_session,
       count(distinct add_session_id) as goods_add_sesson,
       count(distinct if(list_type in('list-category','list-pre-order'),impression_session_id,null)) as cy_po_impreesion,
       count(distinct if(list_type in('list-category','list-pre-order'),click_session_id,null)) as cy_po_click,
       count(distinct detail_add_session_id) detail_add,
       count(distinct detail_view_session_id) detail_view,
       count(order_id) order_paid_number,
       sum(goods_number*shop_price*100) goods_amount

from (
         select t1.project as project_name,
                t1.platform,
                t1.country as country_code,
                t1.dvce_type as device_type,
                t1.list_type,
                t1.virtual_goods_id,
                add_session_id,
                click_session_id,
                impression_session_id,
                detail_add_session_id,
                detail_view_session_id,
                goods_id,
                cat_name,
                null order_id,
                null goods_number,
                null shop_price,
                null pay_status
         from (select project,
                      country,
                      platform,
                      dvce_type,
                      single_goods_event.list_type                        AS           list_type,
                      case
                          when event_name IN ('goods_click', 'goods_impression') then CAST(single_goods_event.virtual_goods_id AS INT)
                          when event_name IN ('page_view', 'screen_view') and page_code = 'product'
                              then CAST(url_virtual_goods_id AS INT)
                          when event_name IN ('add', 'checkout', 'remove') then CAST(single_ecommerce_event.id AS INT)
                          end                                             AS           virtual_goods_id,
                      if(event_name = 'add', session_id, null)              as           add_session_id,
                      if(event_name = 'goods_click', session_id, null)      AS           click_session_id,
                      if(event_name = 'goods_impression', session_id, null) AS           impression_session_id,
                      if(event_name in ('page_view', 'screen_view') and page_code = 'product', session_id,'')  detail_view_session_id,
                      if(event_name = 'add' and page_code = 'product', session_id, null)  detail_add_session_id
               from ods_fd_snowplow.ods_fd_snowplow_all_event
                        lateral view outer explode(goods_event_struct) tmp1 as single_goods_event
                        lateral view outer explode(ecommerce_product) tmp2 AS single_ecommerce_event
               where event_name in
                     ('goods_click', 'goods_impression', 'screen_view', 'page_view', 'add', 'checkout', 'remove')
                 and pt = '2020-12-07') t1
     inner join  dim.dim_fd_goods gi on t1.virtual_goods_id = gi.virtual_goods_id and t1.project = gi.project_name
                                    where t1.virtual_goods_id is not null

      union all

     select  project_name,
                     if(is_app is null, 'other', if(is_app = 0, 'web', 'mob')) as platform,
                     country_code,
                     case device_type
                         when 'mobile' then 'Mobile'
                         when 'pad' then 'Tablet'
                         when 'pc' then 'Computer'
                         when 'unknown' then 'Unknown'
                         else 'Game console' end as device_type,
                     null                           list_type,
                     cast(virtual_goods_id as int)  virtual_goods_id,
                     null                        as add_sesson_id,
                     null                        as click_session_id,
                     null                        as impression_session_id,
                     null                        as detail_add_session_id,
                     null                        as detail_view_session_id,
                     cast(goods_id as int)          goods_id,
                     cat_name,
                     order_id,
                     goods_number,
                     shop_price,
                     pay_status
     from dwd.dwd_fd_order_goods
     where pay_time is not null
     and date(from_unixtime(pay_time))='2020-12-07'
       and pay_status = 2
       and virtual_goods_id is not null
       and email is not null
       and email not like '%i9i8.com'
       and email not like '%tetx.com'
    )t2

group by project_name,platform, country_code,device_type, virtual_goods_id, cat_name, goods_id;