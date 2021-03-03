insert overwrite table dwb.dwb_fd_goods_test_finder_rpt
select
	/*+ REPARTITION(1) */
    gt_all.project_name as project, --组织
    if(gt_all.cat_name is null or trim(gt_all.cat_name)='','null',gt_all.cat_name) as cat_name, --品类名
    test_type,--测款类型
    if(dfg.goods_selector is null or trim(dfg.goods_selector )='','null',dfg.goods_selector ) as finder ,--选款人
    preorder_plan_name,--预售计划
    gt_all.virtual_goods_id,
    test_finish_dt,--测款结束时间
    result,
    nvl(last_7_days_goods_sales,0) as last_7_days_goods_sales,
    nvl(last_7_days_cat_sales,0) as last_7_days_cat_sales
from
(
	--测款成功
    select
            gtg.project_name,
            g.cat_id,
            g.cat_name,
            '快速测款'  as test_type,
            g.virtual_goods_id,
            to_date(to_utc_timestamp(gtg.end_time, 'America/Los_Angeles')) as test_finish_dt,--utc时间
            1 as result,
            '' as preorder_plan_name,
            gtg.goods_id
    from  ods_fd_vb.ods_fd_goods_test_goods_report gtg
    left join dim.dim_fd_goods g on g.goods_id = gtg.goods_id and lower(g.project_name) = lower(gtg.project_name)
    where gtg.result=1

    union all

    --测款失败
    select
            gtg.project_name,
            g.cat_id,
            g.cat_name,
            '快速测款'  as test_type,
            g.virtual_goods_id,
            to_date(to_utc_timestamp(gtg.end_time, 'America/Los_Angeles')) as test_finish_dt,--utc时间
            0 as result,
            '' as preorder_plan_name,
            gtg.goods_id
    from  ods_fd_vb.ods_fd_goods_test_goods_report gtg
    left join dim.dim_fd_goods g on g.goods_id = gtg.goods_id and lower(g.project_name) = lower(gtg.project_name)
    where gtg.result=2

    union all

    select
            project_name,
            cat_id,
            cat_name,
           '预售测款' as test_type,
            virtual_goods_id,
            to_date(finish_time) as test_finish_dt, --utc时间
            result,
            preorder_plan_name,
            goods_id
    from dwd.dwd_fd_finished_preorder

) gt_all

left join dim.dim_fd_goods dfg on dfg.goods_id = gt_all.goods_id and dfg.project_name = gt_all.project_name

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
