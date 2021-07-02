insert overwrite table dwb.dwb_fd_achievements_basic partition (pt='${pt}')
select
 /*+ REPARTITION(1) */
tab1.cat_name,
tab1.country_code,
tab1.goods_id,
tab1.cat_id,
tab1.shop_price,
tab1.goods_number,
tab1.order_time_original,
tab1.project_name,
finder,
test_finish_dt
from
(
    select
        cat_name,
        country_code,
        goods_id,
        cat_id,
        shop_price,
        goods_number,
        to_date(order_time_original) order_time_original,
        project_name
    from
        dwd.dwd_fd_order_goods
    where
        month(to_date(order_time_original)) = month(date_sub('${pt}',1))
        and project_name in ('floryday','airydress')
        and cat_id in (195,164,162,168,174,3001)
        and pay_status = 2
)tab1

left join
(
    select
        project_name,
        goods_id,
        to_date( end_time ) as test_finish_dt
    from
        ods_fd_vb.ods_fd_goods_test_goods_report
    where
        end_time is not null
    group by
        project_name,
        goods_id,
        end_time
)tab2
on
    tab1.goods_id = tab2.goods_id
    and lower(tab1.project_name) = lower(tab2.project_name)

left join
(
    select
        project_name,
        goods_id,
        goods_selector as finder
    from
        dim.dim_fd_goods
    where
        goods_selector is not null
    group by
        project_name,
        goods_id,
        goods_selector
)tab3
on
    tab1.goods_id = tab3.goods_id
    and lower(tab1.project_name) = lower(tab3.project_name);