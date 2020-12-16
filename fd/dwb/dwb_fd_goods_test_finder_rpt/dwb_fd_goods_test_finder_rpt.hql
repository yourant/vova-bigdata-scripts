
insert overwrite table dwb.dwb_fd_goods_test_finder_rpt

SELECT

    t1.project_name,
    t1.finder,
    t1.test_time,
    t1.cat_name,
    t1.test_type,
    t1.preorder_plan_name,
    finished_goods_num,
    success_goods_num,
    success_goods_sales_amount_7d,
    cat_sales_amount_7d,
    hot_style_num

from(

select
    test_goods_sales.project_name,
    finder,
    test_time,
    test_goods_sales.cat_name,
    test_type,
    preorder_plan_name,
    finished_goods_num,
    success_goods_num,
    success_goods_sales_amount_7d,
    cat_sales_amount_7d
from
(

select project_name,
       finder,
       test_time,
       nvl(cat_name,'all')                                    as cat_name,
       nvl(test_type, 'all')                                  as test_type,
       nvl(preorder_plan_name, 'all')                         as preorder_plan_name,
       count(distinct virtual_goods_id)                       as finished_goods_num,
       count(distinct if(result = 1, virtual_goods_id, null)) as success_goods_num,
       sum(if(result = 1, goods_sales_7d, 0))                 as success_goods_sales_amount_7d

from
(
     select
        test_time,
        nvl(goods_test.project_name,'NALL') as project_name,
        nvl(cat_name,'NALL')           as cat_name,
        goods_test.virtual_goods_id    as virtual_goods_id,
        result,
        nvl(test_type,'NALL')          as test_type,
        nvl(preorder_plan_name,'NALL') as preorder_plan_name,
        nvl(f.finder,'NALL')           as finder,
        goods_sales_7d

from
(
            SELECT
                project_name,
                cat_name,
                virtual_goods_id,
                result,
                '快速测款' as test_type,
                null   as preorder_plan_name,
                date(test_time) as test_time
            from dwd.dwd_fd_finished_goods_test where test_time is not null

            union ALL

            SELECT
                project_name,
                cat_name,
                virtual_goods_id,
                result,
                '预售测款' as test_type,
                preorder_plan_name,
                date(test_time) as test_time
            from dwd.dwd_fd_finished_preorder
)goods_test
 left join dim.dim_fd_goods_finder f
on f.virtual_goods_id = goods_test.virtual_goods_id
left join(
    select virtual_goods_id,
          sum(goods_number * shop_price) as goods_sales_7d
    from dwd.dwd_fd_order_goods
    where pay_status = 2
    and date(from_unixtime(pay_time)) between date_add('${pt}', -6) and date_add('${pt}', 1)
    group by virtual_goods_id
    ) goods_sales on goods_sales.virtual_goods_id = goods_test.virtual_goods_id


)goods
group by test_time,project_name, finder,cat_name, test_type, preorder_plan_name
grouping sets(
    (test_time,project_name,finder,cat_name,test_type, preorder_plan_name),
    (test_time,project_name,finder,cat_name,test_type),
    (test_time,project_name,finder,cat_name),
    (test_time,project_name,finder))
)test_goods_sales

left join
(
            select cat_name,
                   project_name,
                   sum(goods_number * shop_price) as cat_sales_amount_7d
            from dwd.dwd_fd_order_goods
            where pay_status = 2
            and date(from_unixtime(pay_time)) between date_add('${pt}', -6) and date_add('${pt}', 1)
            group by cat_name,project_name

)cat_sales  on test_goods_sales.cat_name=cat_sales.cat_name
                and test_goods_sales.project_name=cat_sales.project_name

)t1


left join

(
SELECT
    nvl(success_goods_sales.project_name,'all') as project_name,
    test_time,
    nvl(finder,'all')                           as finder,
    nvl(success_goods_sales.cat_name,'all')     as cat_name,
    nvl(test_type,'all')                        as test_type,
    nvl(preorder_plan_name,'all')               as preorder_plan_name,
    count(distinct if( round(goods_sales_7d,4)/cat_sales_amount_7d >0.01,virtual_goods_id,null)) as hot_style_num

from(

    SELECT
        nvl(success_goods_test.project_name,'NALL') as project_name,
        nvl(finder,'NALL')                          as finder,
        nvl(test_time,'NALL')                       as test_time,
        nvl(success_goods_test.cat_name,'NALL')     as cat_name,
        nvl(test_type,'NALL')                       as  test_type,
        nvl(preorder_plan_name,'NALL')              as preorder_plan_name,
        success_goods_test.virtual_goods_id,
        goods_sales_7d
    FROM
    (
    SELECT
        project_name,
        cat_name,
        virtual_goods_id,
        '快速测款' as test_type,
        null   as preorder_plan_name,
        date(test_time) as test_time
    from dwd.dwd_fd_finished_goods_test where test_time is not null and result=1

    union ALL

    SELECT
        project_name,
        cat_name,
        virtual_goods_id,
        '预售测款' as test_type,
        preorder_plan_name,
        date(test_time) as test_time
    from dwd.dwd_fd_finished_preorder where result=1
    )success_goods_test

    left join dim.dim_fd_goods_finder f
    on f.virtual_goods_id = success_goods_test.virtual_goods_id
    left join(
        select virtual_goods_id,
            sum(goods_number * shop_price) as goods_sales_7d
        from dwd.dwd_fd_order_goods
        where pay_status = 2
        and date(from_unixtime(pay_time)) between date_add('${pt}', -6) and date_add('${pt}', 1)
        group by virtual_goods_id
        ) goods_sales on goods_sales.virtual_goods_id = success_goods_test.virtual_goods_id

)success_goods_sales
 left join (
    select cat_name,
           project_name,
           sum(goods_number * shop_price) as cat_sales_amount_7d
    from dwd.dwd_fd_order_goods
    where pay_status = 2
    and date(from_unixtime(pay_time)) between date_add('${pt}', -6) and date_add('${pt}', 1)
    group by cat_name,project_name

 )cat_sales   on success_goods_sales.project_name=cat_sales.project_name and success_goods_sales.cat_name=cat_sales.cat_name
 group by       test_time,
                success_goods_sales.project_name,
                finder,
                success_goods_sales.cat_name,
                test_type,
                preorder_plan_name
grouping sets(
    (test_time,success_goods_sales.project_name,finder,success_goods_sales.cat_name,test_type,preorder_plan_name),
    (test_time,success_goods_sales.project_name,finder,success_goods_sales.cat_name,test_type),
    (test_time,success_goods_sales.project_name,finder,success_goods_sales.cat_name),
    (test_time,success_goods_sales.project_name,finder))

)t2   on t1.project_name=t2.project_name and t1.cat_name=t2.cat_name and t1.test_type=t2.test_type
         and  t1.preorder_plan_name=t2.preorder_plan_name
         and t1.test_time=t2.test_time and t1.finder=t2.finder;