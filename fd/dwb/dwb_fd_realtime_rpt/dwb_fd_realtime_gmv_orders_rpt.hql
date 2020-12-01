insert overwrite table dwb.dwb_fd_realtime_rpt partition(pt='${pt}',class='orders_number')
SELECT
         /*+ REPARTITION(1) */
               nvl(project,'all'),
               nvl(platform,'all'),
               nvl(country,'all'),
           sum(if(hour = 0, 1, 0))             as h0,
           sum(if(hour = 1, 1, 0))             as h1,
           sum(if(hour = 2, 1, 0))             as h2,
           sum(if(hour = 3, 1, 0))             as h3,
           sum(if(hour = 4, 1, 0))             as h4,
           sum(if(hour = 5, 1, 0))             as h5,
           sum(if(hour = 6, 1, 0))             as h6,
           sum(if(hour = 7, 1, 0))             as h7,
           sum(if(hour = 8, 1, 0))             as h8,
           sum(if(hour = 9, 1, 0))             as h9,
           sum(if(hour = 10, 1, 0))            as h10,
           sum(if(hour = 11, 1, 0))            as h11,
           sum(if(hour = 12, 1, 0))            as h12,
           sum(if(hour = 13, 1, 0))            as h13,
           sum(if(hour = 14, 1, 0))            as h14,
           sum(if(hour = 15, 1, 0))            as h15,
           sum(if(hour = 16, 1, 0))            as h16,
           sum(if(hour = 17, 1, 0))            as h17,
           sum(if(hour = 18, 1, 0))            as h18,
           sum(if(hour = 19, 1, 0))            as h19,
           sum(if(hour = 20, 1, 0))            as h20,
           sum(if(hour = 21, 1, 0))            as h21,
           sum(if(hour = 22, 1, 0))            as h22,
           sum(if(hour = 23, 1, 0))            as h23
from
(

        select
                     paid_time,
                     hour(paid_time) as hour,
                     oi.project_name     as project,
                     is_app,
                     device_type,
                     os_type,
                     case
                         when is_app = 0 and device_type = 'pc' then 'PC'
                         when is_app = 0 and device_type = 'mobile' then 'H5'
                         when is_app = 0 and device_type = 'pad' then 'Tablet'
                         when is_app = 1 and os_type = 'android' then 'Android'
                         when is_app = 1 and os_type = 'ios' then 'IOS'
                         else 'Others'
                     end                                                          as platform,
                     r.region_code                                                as country,
                     oi.goods_amount + oi.shipping_fee                            as gmv
        from (
            select  date_format(to_utc_timestamp(pay_time, 'America/Los_Angeles'),'yyyy-MM-dd HH:mm:ss') as paid_time,
            project_name,order_id,country,user_agent_id,email,goods_amount,shipping_fee from ods_fd_vb.ods_fd_order_info_inc
            where pt='${pt}'  and  pay_status=2 and pay_time is not null
            and    to_date(date_format(to_utc_timestamp(pay_time, 'America/Los_Angeles'),'yyyy-MM-dd HH:mm:ss'))='${pt}'
                        and  email NOT REGEXP "tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com"
        ) oi

        left join  ods_fd_vb.ods_fd_user_agent_analysis uaa on oi.user_agent_id=uaa.user_agent_id
        left join dim.dim_fd_region r on r.region_id = oi.country

    )tab1
group by  project, platform, country with cube;




insert overwrite table dwb.dwb_fd_realtime_rpt partition(pt='${pt}',class='gmv')
SELECT

              nvl(project,'all'),
              nvl(platform,'all'),
              nvl(country,'all'),
           sum(if(hour = 0, gmv, 0))             as h0,
           sum(if(hour = 1, gmv, 0))             as h1,
           sum(if(hour = 2, gmv, 0))             as h2,
           sum(if(hour = 3, gmv, 0))             as h3,
           sum(if(hour = 4, gmv, 0))             as h4,
           sum(if(hour = 5, gmv, 0))             as h5,
           sum(if(hour = 6, gmv, 0))             as h6,
           sum(if(hour = 7, gmv, 0))             as h7,
           sum(if(hour = 8, gmv, 0))             as h8,
           sum(if(hour = 9, gmv, 0))             as h9,
           sum(if(hour = 10, gmv, 0))            as h10,
           sum(if(hour = 11, gmv, 0))            as h11,
           sum(if(hour = 12, gmv, 0))            as h12,
           sum(if(hour = 13, gmv, 0))            as h13,
           sum(if(hour = 14, gmv, 0))            as h14,
           sum(if(hour = 15, gmv, 0))            as h15,
           sum(if(hour = 16, gmv, 0))            as h16,
           sum(if(hour = 17, gmv, 0))            as h17,
           sum(if(hour = 18, gmv, 0))            as h18,
           sum(if(hour = 19, gmv, 0))            as h19,
           sum(if(hour = 20, gmv, 0))            as h20,
           sum(if(hour = 21, gmv, 0))            as h21,
           sum(if(hour = 22, gmv, 0))            as h22,
           sum(if(hour = 23, gmv, 0))            as h23
from
(

        select
                     paid_time,
                     hour(paid_time) as hour,
                     oi.project_name     as project,
                     is_app,
                     device_type,
                     os_type,
                     case
                         when is_app = 0 and device_type = 'pc' then 'PC'
                         when is_app = 0 and device_type = 'mobile' then 'H5'
                         when is_app = 0 and device_type = 'pad' then 'Tablet'
                         when is_app = 1 and os_type = 'android' then 'Android'
                         when is_app = 1 and os_type = 'ios' then 'IOS'
                         else 'Others'
                     end                                                          as platform,
                     r.region_code                                                as country,
                     oi.goods_amount + oi.shipping_fee                            as gmv
        from (
            select  date_format(to_utc_timestamp(pay_time, 'America/Los_Angeles'),'yyyy-MM-dd HH:mm:ss') as paid_time,
            project_name,order_id,country,user_agent_id,email,goods_amount,shipping_fee from ods_fd_vb.ods_fd_order_info_inc
            where pt='${pt}'  and  pay_status=2 and pay_time is not null
            and    to_date(date_format(to_utc_timestamp(pay_time, 'America/Los_Angeles'),'yyyy-MM-dd HH:mm:ss'))='${pt}'
                        and  email NOT REGEXP "tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com"
        ) oi

        left join  ods_fd_vb.ods_fd_user_agent_analysis uaa on oi.user_agent_id=uaa.user_agent_id
        left join dim.dim_fd_region r on r.region_id = oi.country

    )tab1
group by  project, platform, country with cube;
