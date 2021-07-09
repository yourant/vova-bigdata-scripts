msck repair table ads.ads_fd_royalty_threshold_d;
insert overwrite table dwb.dwb_fd_commission_standard partition( pt = '${pt}')
select
    project_name,
    cat_name,
    project,
    commission,
    month_sale_threshold,
    rank_threshold,
    dynamic_goods_num
from
(
    select
        tab1.project_name,
        tab1.cat_name,
        '常规' as project,
        200 as commission,
        tab2.month_sale_threshold,
        tab2.rank_threshold,
        tab1.dynamic_goods_num
    from
    (
        select
            project_name,
            cat_name,
            cat_id,
            '0' as project,
            count( distinct goods_id ) dynamic_goods_num --动销商品数量
        from
            dwb.dwb_fd_achievements_basic
        where
            pt = '${pt}'
            and goods_number > 0
        group by
            project_name,
            cat_name,
            cat_id
    )tab1

    left join
    (
        select
            datasource as project_name,
            cat_id,
            region_code as project,
            month_sale_threshold,
            rank_threshold
        from
            ads.ads_fd_royalty_threshold_d
        where
            pt = '${pt}'
            and region_code = '0'
    )tab2
    on
        tab1.project_name = tab2.project_name
        and tab1.cat_id = tab2.cat_id
        and tab1.project = tab2.project
)

union all
(
    select
        tab1.project_name,
        tab1.cat_name,
        '美国' as project ,
        300 as commission,
        tab2.month_sale_threshold,
        tab2.rank_threshold,
        tab1.dynamic_goods_num
    from
    (
        select
            project_name,
            cat_name,
            cat_id,
            'US' as project,
            count( distinct goods_id ) dynamic_goods_num --动销商品数量
        from
            dwb.dwb_fd_achievements_basic
        where
            pt = '${pt}'
            and goods_number > 0
            and country_code = 'US'
        group by
            project_name,
            cat_name,
            cat_id
    )tab1

    left join
    (
        select
            datasource as project_name,
            cat_id,
            region_code as project,
            month_sale_threshold,
            rank_threshold
        from
            ads.ads_fd_royalty_threshold_d
        where
            pt = '${pt}'
            and region_code = 'US'
    )tab2
    on
        tab1.project_name = tab2.project_name
        and tab1.cat_id = tab2.cat_id
        and tab1.project = tab2.project
);