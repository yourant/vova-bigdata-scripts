insert overwrite table dwb.dwb_fd_goods_test_finder_rpt
select
    /*+ REPARTITION(1) */
    gt_all.project_name,
    if(cat_name is null or trim(cat_name)='','null',cat_name) as cat_name,
    test_type,
    if(gpe.ext_value is null or trim(gpe.ext_value )='','null',gpe.ext_value ) as finder ,
    preorder_plan_name,
    gt_all.virtual_goods_id,
    test_finish_dt,
    result,
    nvl(last_7_days_goods_sales,0) as last_7_days_goods_sales,
    nvl(last_7_days_cat_sales,0) as last_7_days_cat_sales
from
(
    select
            project_name,
            cat_id,
            cat_name,
            '快速测款'  as test_type,
            virtual_goods_id,
            cast(min(to_date(finish_time))  as string) as test_finish_dt,
            1 as result,
            '' as preorder_plan_name,
            goods_id
    from  dwd.dwd_fd_goods_test_thread_single
    where result=1
    group by project_name,cat_id,cat_name,virtual_goods_id,goods_id

    union all

    select
        project_name,
        cat_id,
        cat_name,
        '快速测款'  as test_type,
        virtual_goods_id,
        test_finish_dt,
        0     as result,
        '' as preorder_plan_name,
        goods_id
from
(
        select project_name,
            cat_id,
            cat_name,
            virtual_goods_id,
            cast(max(to_date(finish_time)) as string)  as test_finish_dt,
            concat_ws(',',collect_set(cast(result as string))) as all_result,
            '快速测款'  as test_type,
            goods_id
    from  dwd.dwd_fd_goods_test_thread_single
    where result != 1
    group by project_name,cat_id,cat_name,virtual_goods_id,goods_id
)gt_finish
where all_result not regexp '0|8'

    union all

    select
            project_name,
            cat_id,
            cat_name,
           '预售测款' as test_type,
            virtual_goods_id,
            finish_time as test_finish_dt,
            result,
            preorder_plan_name,
            goods_id
    from dwd.dwd_fd_finished_preorder
) gt_all

left join
         dwd.dwd_fd_goods_project_extension gpe
on gpe.goods_id = gt_all.goods_id and gpe.project_name = gt_all.project_name and gpe.ext_name = 'goods_selector'

left join
(
    select virtual_goods_id,
        sum(goods_number * shop_price) as last_7_days_goods_sales
    from dwd.dwd_fd_order_goods
    where pay_status = 2
    and from_unixtime(pay_time) between date_add('${pt}',-6) and date_add('${pt}', 1)
group by virtual_goods_id
)gt_sales
on gt_all.virtual_goods_id=gt_sales.virtual_goods_id

left join
(
    select
        cat_id,
        project_name,
        sum(goods_number * shop_price) as last_7_days_cat_sales
    from dwd.dwd_fd_order_goods
    where pay_status = 2
    and from_unixtime(pay_time) between date_add('${pt}',-6) and date_add('${pt}', 1)
    group by cat_id, project_name
)gt_cat_sales
on  gt_all.cat_id=gt_cat_sales.cat_id and gt_all.project_name=gt_cat_sales.project_name
where gt_all.cat_name is not null ;
