insert overwrite table dwb.dwb_fd_commission_summary partition (pt = '${pt}')
select
    /*+ REPARTITION(1) */
    project_name,
    cat_name,
    project,
    finder,
    commission_goods_number,
    commission_goods_amount
from
(
    select
        project_name,
        cat_name,
        project,
        finder,
        commission_goods_number,
        case
            when project = '常规' then 200*commission_goods_number
            else 100*commission_goods_number
        end as commission_goods_amount
    from
    (
        select
            /*+ REPARTITION(1) */
            tab1.project_name,
            tab1.cat_name,
            tab1.project,
            tab1.finder,
            count( distinct tab1.goods_id ) commission_goods_number
        from
        (
            select
                project_name,
                cat_name,
                project,
                finder,
                goods_id
            from
                dwb.dwb_fd_reach_achievements_standard_goods_info
            where
                project_name = 'floryday'
                and pt = '${pt}'
        )tab1

        left join
        (
            select
                project_name,
                cat_name,
                project,
                finder,
                goods_id
            from
                dwb.dwb_fd_reach_achievements_standard_goods_info
            where
                project_name = 'airydress'
                and pt = '${pt}'
        )tab2
        on
            tab1.goods_id = tab2.goods_id
            and tab1.project = tab2.project
            and tab1.finder = tab2.finder
        where
            tab2.goods_id is null
        group by
            tab1.project_name,
            tab1.cat_name,
            tab1.project,
            tab1.finder
    )
)

union all
(
    select
        project_name,
        cat_name,
        project,
        finder,
        commission_goods_number,
        case
            when project = '常规' then 200*commission_goods_number
            else 100*commission_goods_number
        end as commission_goods_amount
    from
    (
        select
            /*+ REPARTITION(1) */
            project_name,
            cat_name,
            project,
            finder,
            count( distinct goods_id ) commission_goods_number
        from
        (
            select
                project_name,
                cat_name,
                project,
                finder,
                goods_id
            from
                dwb.dwb_fd_reach_achievements_standard_goods_info
            where
                project_name = 'airydress'
                and pt = '${pt}'
        )
        group by
            project_name,
            cat_name,
            project,
            finder         
    )
);