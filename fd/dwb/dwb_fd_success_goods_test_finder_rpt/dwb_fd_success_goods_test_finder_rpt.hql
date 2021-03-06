
insert overwrite table dwb.dwb_fd_success_goods_test_finder_rpt
select
    /*+ REPARTITION(1) */
    goods_table.project_name,
    finder,
    test_time,
    nvl(cat_name, 'all') as cat_name,
    test_type,
    preorder_plan_name,
    finished_goods_num,
    success_goods_num,
    success_goods_sales_amount_7d,
    cat_sales_amount_7d_all,
    hot_style_num,
    success_month_amount

from (
         select nvl(project_name,'all')                                as project_name,
                nvl(finder,'all')                                      as finder,
                nvl(test_time,'all')                                   as test_time,
                nvl(cat_id, 'all')                                     as cat_id,
                nvl(test_type, 'all')                                  as test_type,
                nvl(preorder_plan_name,'all')                          as preorder_plan_name,
                count(distinct virtual_goods_id)                       as finished_goods_num,
                count(distinct if(result = 1, virtual_goods_id, null)) as success_goods_num,
                sum(if(result = 1, goods_sales_7d, 0))                 as success_goods_sales_amount_7d,
                count(distinct
                      if(result = 1 and cat_sales_amount_7d>0  and (round(goods_sales_7d, 4) / cat_sales_amount_7d) > 0.01, virtual_goods_id,
                         null))                                        as hot_style_num,
                sum(if(result=1,success_month_amount,0))               as success_month_amount

         from (
                  select test_time,
                         nvl(goods_test.project_name, 'NALL') as project_name,
                         nvl(goods_test.cat_id, 'NALL')       as cat_id,
                         goods_test.virtual_goods_id          as virtual_goods_id,
                         result,
                         nvl(test_type, 'NALL')               as test_type,
                         nvl(preorder_plan_name, 'NALL')      as preorder_plan_name,
                         nvl(f.finder, 'NALL')                as finder,
                         nvl(success_month_amount,0)          as success_month_amount,
                         nvl(goods_sales_7d,0)                as goods_sales_7d,
                         nvl(cat_sales_amount_7d,0)           as cat_sales_amount_7d

                  from (
                           SELECT project_name,
                                  cat_id,
                                  virtual_goods_id,
                                  result,
                                  '????????????'                            as test_type,
                                  null                              as preorder_plan_name,
                                  date(test_time)                   as test_time
                           from dwd.dwd_fd_finished_goods_test
                           where test_time is not null and date(test_time)<='${pt}'

                           union ALL

                           SELECT project_name,
                                  cat_id,
                                  virtual_goods_id,
                                  result,
                                  '????????????'                            as test_type,
                                  preorder_plan_name,
                                  date(test_time)                   as test_time
                           from dwd.dwd_fd_finished_preorder
                           where test_time is not null and date(test_time)<='${pt}'
                       ) goods_test
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

                           left join
                       (
                           select cat_id,
                                  project_name,
                                  sum(goods_number * shop_price) as cat_sales_amount_7d
                           from dwd.dwd_fd_order_goods
                           where pay_status = 2
                             and date(from_unixtime(pay_time)) between date_add('${pt}', -6) and date_add('${pt}', 1)
                           group by cat_id, project_name
                       ) cat_sales on goods_test.cat_id = cat_sales.cat_id
                           and goods_test.project_name = cat_sales.project_name

                           left join(
                      select virtual_goods_id,
                             date_format(from_unixtime(pay_time), 'yyyy-MM') as pay_month,
                             sum(goods_number * shop_price)                  as success_month_amount
                      from dwd.dwd_fd_order_goods
                      where pay_status = 2
                      group by virtual_goods_id, date_format(from_unixtime(pay_time), 'yyyy-MM')
                  ) og on goods_test.virtual_goods_id = og.virtual_goods_id
                           and  date_format(goods_test.test_time, 'yyyy-MM')=og.pay_month

              ) detail_table
         group by test_time, project_name, finder, cat_id, test_type, preorder_plan_name
          with cube
     ) goods_table

         left join
     (
         select nvl(cat_id, 'all') as cat_id,
                nvl(project_name,'all') as project_name,
                sum(goods_number * shop_price) as cat_sales_amount_7d_all
         FROM dwd.dwd_fd_order_goods
         where pay_status = 2
           and date(from_unixtime(pay_time)) between date_add('${pt}', -6) and date_add('${pt}', 1)
         group by cat_id, project_name
          with cube
     ) cat_table on goods_table.cat_id = cat_table.cat_id and goods_table.project_name=cat_table.project_name

         left join dim.dim_fd_category dfc on dfc.cat_id = goods_table.cat_id;