insert overwrite table dwd.dwd_fd_category_sales_monthly partition (mt = '${mt}')
select
       /*+ REPARTITION(1) */
       cat_id,
       cat_name,
       project_name,
       platform_name,
       country_code,
       count(distinct order_id)       as order_num,
       sum(goods_number)              as sales_num,
       sum(goods_number * shop_price) as sales_amount
from (
         select cat_id,
                cat_name,
                project_name,
                country_code,
                case
                    when platform_type in ('pc_web', 'tablet_web') then 'PC'
                    when platform_type in ('mobile_web') then 'H5'
                    when platform_type in ('ios_app', 'android_app') then 'APP'
                    else 'Others'
                    end as platform_name,
                order_id,
                goods_number,
                shop_price
         from dwd.dwd_fd_order_goods
         where from_unixtime(pay_time, "YYYY-MM") = "${mt}"
           and pay_status > 0
           and pay_time is not null) orders
group by cat_id, cat_name, project_name, platform_name, country_code;
