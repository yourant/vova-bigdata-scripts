insert overwrite table ads.ads_fd_royalty_threshold_detail_d partition (pt = '${pt}')
select  
    project_name,
    cat_id,
    region_code,
    goods_id,
    avg_goods_amount
from
(
    select
        project_name,
        cat_id,
        '0' as region_code,
        goods_id,
        avg_goods_amount
    from
    (
        select         
            lower(project_name) project_name,
            cat_id,
            '0' as region_code,
            goods_id,
            sum( shop_price*goods_number )/31 as avg_goods_amount,
            sum( goods_number ) as goods_number
        from
            dwd.dwd_fd_order_goods
        where
            month(date_sub(to_date(order_time_original),1)) = month(date_sub('${pt}',1))
            and pay_status = 2
            and lower(project_name) in ('floryday','airydress')
            and cat_id in (195,164,162,168,174,3001)
        group by
            project_name,
            cat_id,
            goods_id
    )
)

union all
(
    select
        project_name,
        cat_id,
        'US' as region_code,
        goods_id,
        avg_goods_amount
    from
    (
        select
            lower(project_name) project_name,
            cat_id,
            'US' as region_code,
            goods_id,
            sum( shop_price*goods_number )/31 as avg_goods_amount,
            sum( goods_number ) as goods_number
        from
            dwd.dwd_fd_order_goods
        where
            month(date_sub(to_date(order_time_original),1)) = month(date_sub('${pt}',1))
            and pay_status = 2
            and lower(project_name) in ('floryday','airydress')
            and cat_id in (195,164,162,168,174,3001)
            and country_code = 'US'
        group by
            project_name,
            cat_id,
            goods_id                
    )
);