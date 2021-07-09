insert overwrite table dwb.dwb_fd_reach_achievements_standard_goods_info partition (pt = '${pt}')
select
    /*+ REPARTITION(1) */
    project_name,
    cat_name,
    project,
    finder,
    goods_id,
    order_time_original,
    goods_amount
from
(
    select
        /*+ REPARTITION(1) */
        project_name,
        cat_name,
        '常规' as project,
        finder,
        goods_id,
        order_time_original,
        goods_amount
    from
    (
        select
            tab1.project_name,
            tab1.cat_name,
            tab1.project,
            tab1.finder,
            tab1.goods_id,
            tab1.order_time_original,
            tab1.goods_amount,
            count( tab1.order_time_original ) over( partition by tab1.cat_name,tab1.project_name,tab1.finder,tab1.goods_id ) count_goods
        from
        (
            select
                project_name,
                cat_name,
                '常规' as project,
                finder,
                goods_id,
                order_time_original,
                sum( shop_price*goods_number ) goods_amount,
                sum( goods_number ) goods_number_amount
            from
                dwb.dwb_fd_achievements_basic
            where
                test_finish_dt >= '2021-03-01'
                and goods_number > 0
                and finder is not null
                and pt = '${pt}'
            group by
                project_name,
                cat_name,
                finder,
                goods_id,
                order_time_original
        )tab1

        left join
        (
            select
                pt,
                project_name,
                cat_name,
                threshold,
                project,
                ordinal
            from
                dwb.dwb_fd_commission_standard
        )tab2
        on
            tab1.cat_name = tab2.cat_name
            and tab1.project_name = tab2.project_name
            and tab1.project = tab2.project

        left join
        (
            select
                goods_id,
                finder,
                project,
                cat_name
            from
                dwb.dwb_fd_reach_achievements_standard_goods_info
            where
                pt < '${pt}'
        )tab3
        on
            tab1.goods_id = tab3.goods_id
            and tab1.finder = tab3.finder
            and tab1.project = tab3.project
            and tab1.cat_name = tab3.cat_name
        where
            tab1.goods_amount > tab2.threshold
            and tab1.goods_number_amount >= 2
            and tab2.project = '常规'
            and tab3.goods_id is null
            and tab2.pt = '${pt}'

    )tab
    where
        tab.count_goods >= 7
)

union all
(
    select
        /*+ REPARTITION(1) */
        project_name,
        cat_name,
        '美国' as project,
        finder,
        goods_id,
        order_time_original,
        goods_amount
    from
    (
        select
            tab1.project_name,
            tab1.cat_name,
            tab1.project,
            tab1.finder,
            tab1.goods_id,
            tab1.order_time_original,
            tab1.goods_amount,
            count( tab1.order_time_original ) over( partition by tab1.cat_name,tab1.project_name,tab1.finder,tab1.goods_id ) count_goods
        from
        (
            select
                project_name,
                cat_name,
                '美国' as project,
                finder,
                goods_id,
                order_time_original,
                sum( shop_price*goods_number ) goods_amount,
                sum( goods_number ) goods_number_amount
            from
                dwb.dwb_fd_achievements_basic
            where
                test_finish_dt >= '2021-03-01'
                and goods_number > 0
                and country_code = 'US'
                and finder is not null
                and pt = '${pt}'
            group by
                project_name,
                cat_name,
                finder,
                goods_id,
                order_time_original
        )tab1

        left join
        (
            select
                pt,
                project_name,
                cat_name,
                threshold,
                project,
                ordinal
            from
                dwb.dwb_fd_commission_standard
            where
                project = '美国'
        )tab2
        on
            tab1.cat_name = tab2.cat_name
            and tab1.project_name = tab2.project_name
            and tab1.project = tab2.project

        left join
        (
            select
                goods_id,
                finder,
                project,
                cat_name
            from
                dwb.dwb_fd_reach_achievements_standard_goods_info
            where
                pt < '${pt}'
        )tab3
        on
            tab1.goods_id = tab3.goods_id
            and tab1.finder = tab3.finder
            and tab1.project = tab3.project
            and tab1.cat_name = tab3.cat_name
        where
            tab1.goods_amount > tab2.threshold
            and tab1.goods_number_amount >= 2
            and tab2.project = '美国'
            and tab3.goods_id is null
            and tab2.pt = '${pt}'
    )tab
    where
        tab.count_goods >= 7
);