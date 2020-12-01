insert overwrite table dwd.dwd_fd_goods_performance_monthly partition (mt = '${mt}')
select
    /*+ REPARTITION(1) */
       goods_id,
       virtual_goods_id,
       project_name,
       country,
       platform_name,
       nvl(sum(add_uv), 0)         as add_uv,
       nvl(sum(detail_add_uv), 0)  as detail_add_uv,
       nvl(sum(detail_view_uv), 0) as detail_view_uv,
       nvl(sum(order_num), 0)      as order_num,
       nvl(sum(sales_num), 0)      as sales_num,
       nvl(sum(sales_amount), 0)   as sales_amount
from (
         select goods_id,
                virtual_goods_id,
                project                                                                         as project_name,
                country,
                platform_name,
                count(distinct if(goods_event_name = 'add', session_id, null))                  as add_uv,
                count(distinct
                      if(goods_event_name = 'add' and page_code = 'product', session_id, null)) as detail_add_uv,
                count(distinct if(goods_event_name = 'view', session_id, null))                 as detail_view_uv,
                null                                                                            as order_num,
                null                                                                            as sales_num,
                null                                                                            as sales_amount
         from dwd.dwd_fd_goods_snowplow_performance gsp
         where pt like '${mt}'
           and goods_event_name in ('add', 'view')
         group by goods_id,
                  virtual_goods_id,
                  project,
                  country,
                  platform_name
         union all
         select goods_id,
                virtual_goods_id,
                project_name,
                country,
                platform_name,
                null                     as add_uv,
                null                     as detail_add_uv,
                null                     as detail_view_uv,
                count(distinct order_id) as order_num,
                sum(sales_num)           as sales_num,
                sum(sales_amount)        as sales_amount
         from (
                  select goods_id,
                         virtual_goods_id,
                         from_unixtime(pay_time, "YYYY-MM-dd") as pay_time,
                         project_name,
                         country_code                          as country,
                         case
                             when platform_type in ('pc_web', 'tablet_web') then 'PC'
                             when platform_type in ('mobile_web') then 'H5'
                             when platform_type in ('ios_app', 'android_app') then 'APP'
                             else 'Others'
                             end                               as platform_name,
                         order_id,
                         goods_number                          as sales_num,
                         goods_number * shop_price             as sales_amount
                  from dwd.dwd_fd_order_goods
                  where from_unixtime(pay_time, "YYYY-MM") = "${mt}"
                    and pay_status > 0
                    and pay_time is not null) base
         group by goods_id, virtual_goods_id, project_name, country, platform_name) base
group by goods_id, virtual_goods_id, project_name, country, platform_name;