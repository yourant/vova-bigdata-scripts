insert overwrite table dwb.dwb_fd_newsletter_order_rpt partition (pt = '${hiveconf:pt_last}')
select oi.project_name as project,
       oi.order_date as order_date_utc,
       oi.order_id as order_id,
       nvl(oi.order_sn,oa.order_sn) as order_sn,
       oa.country as country_name,
       nvl(oi.country_code,oa.region_code) as country_code,
       oi.platform_type as platform_type,
       oa.nl_code as nl_code,
       oi.goods_id as goods_id,
       oi.virtual_goods_id as virtual_goods_id,
       oi.cat_name as cat_name,
       oi.goods_number as goods_number,
       (oi.shop_price * oi.goods_number) as shop_price

from  (
  select
    lower(project_name) as project_name,
    country_code as country_code,
    platform_type,
    order_id,
    goods_id,
    goods_number,
    shop_price,
    virtual_goods_id,
    cat_name,
    order_sn,
    date(from_unixtime(order_time,'yyyy-MM-dd HH:mm:ss')) as order_date
  from dwd.dwd_fd_order_goods
  where pay_status = 2
    and date(from_unixtime(order_time,'yyyy-MM-dd HH:mm:ss')) = '${hiveconf:pt_last}'

) oi
left join (

    select t2.order_sn,t2.country,t2.nl_code,t2.order_id,t2.region_code 
    from (
        select t1.order_sn,t1.country,t1.nl_code,t1.order_id,t1.region_code
        from (
            select
                t0.order_sn as order_sn,
                t0.country as country,
                substr(t0.campaign,12) as nl_code,
                t0.order_id as order_id,
                t1.region_code as region_code,
                Row_Number() OVER (partition by oa_id ORDER BY t0.last_update_time desc) rank
            from (select oa_id,order_sn,country,campaign,order_id,ga_channel,last_update_time from ods_fd_ar.ods_fd_order_analytics) t0
            left join dim.dim_fd_region t1 on t1.region_name_en = t0.country
            where (split(t0.campaign, '_')[0] = 'NewsLetter' or t0.ga_channel = 'EDM')
            and substr(t0.campaign,12) !=''
        )t1 where rank = 1
    )t2
) oa on oi.order_id = oa.order_id
where oa.nl_code is not null;
