select project_name,
       finder,
       cat_name,
       test_type,
       preorder_plan_name,
       finished_goods_num,
       success_goods_num,
       success_goods_sales_amount_7d,
       category_sales_7d

from (
         select project_name,
                finder,
                cat_id,
                cat_name,
                nvl(test_type, 'ALL')                                  as test_type,
                nvl(preorder_plan_name, 'ALL')                         as preorder_plan_name,
                count(distinct virtual_goods_id)                       as finished_goods_num,
                count(distinct if(result = 1, virtual_goods_id, null)) as success_goods_num,
                sum(goods_sales_7d)                                    as success_goods_sales_amount_7d
         from (
                  select nvl(project_name, 'NALL')       as project_name,
                         nvl(cat_name, 'NALL')           as cat_name,
                         nvl(test_type, 'NALL')          as test_type,
                         nvl(preorder_plan_name, 'NALL') as preorder_plan_name,
                         nvl(f.finder, 'NALL')           as finder,
                         cat_id,
                         goods_test.virtual_goods_id     as virtual_goods_id,
                         result,
                         goods_sales_7d
                  from (
                           select project_name,
                                  virtual_goods_id,
                                  cat_id,
                                  cat_name,
                                  result,
                                  '快速测款' as test_type,
                                  null   as preorder_plan_name
                           from dwd.dwd_fd_finished_goods_test
                           where finish_time between date_sub('${pt}', 30) and date_add('${pt}', 1)

                           union all

                           select project_name,
                                  virtual_goods_id,
                                  cat_id,
                                  cat_name,
                                  result,
                                  '预售测款' as test_type,
                                  preorder_plan_name

                           from dwd.dwd_fd_finished_preorder
                           where finish_time between date_sub('${pt}', 30) and date_add('${pt}', 1)) as goods_test
                           left join dim.dim_fd_goods_finder f
                                     on f.virtual_goods_id = goods_test.virtual_goods_id
                           left join (
                      select virtual_goods_id,
                             sum(goods_number * shop_price) as goods_sales_7d
                      from dwd.dwd_fd_order_goods
                      where pay_status = 2
                        and pay_time between date_sub('${pt}', 6) and date_add('${pt}', 1)
                      group by virtual_goods_id
                  ) goods_sales on goods_sales.virtual_goods_id = goods_test.virtual_goods_id
              ) goods
         group by project_name, finder, cat_id, cat_name, test_type, preorder_plan_name
             grouping sets (
             ( project_name, finder, cat_id, cat_name, test_type, preorder_plan_name),
             ( project_name, finder, cat_id, cat_name, test_type),
             ( project_name, finder, cat_id, cat_name)
             )) finder_test
         left join (
    select cat_id,
           sum(goods_number * shop_price) as category_sales_7d
    from dwd.dwd_fd_order_goods
    where pay_status = 2
      and pay_time between date_sub('${pt}', 6) and date_add('${pt}', 1)
    group by cat_id
) category_sales on category_sales.cat_id = finder_test.cat_id;