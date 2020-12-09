insert overwrite table dwb.dwb_fd_goods_test_sales_ratio partition (mt='${mt}')
select
/*+ REPARTITION(1) */
       tmp2.project_name,
       tmp2.country_code,
       tmp2.platform_name,
       tmp2.cat_id,
       tmp2.cat_name,
       tmp2.finished_thread_num,
       tmp2.success_thread_num,
       tmp2.finished_goods_num,
       tmp2.success_goods_num,
       tmp2.sum_add_uv_1m,
       tmp2.sum_detail_add_uv_1m,
       tmp2.sum_detail_view_uv_1m,
       tmp2.sum_sales_num_1m,
       tmp2.sum_sales_amount_1m,
       tmp2.sum_sales_amount_2m,
       tmp2.sum_sales_amount_3m,
       csm1.sales_amount,
       csm2.sales_amount,
       csm3.sales_amount


from (
         select project_name,
                country_code,
                platform_name,
                cat_id,
                cat_name,
                count(*)                                             as finished_thread_num,
                sum(if(result in (1, 6), 1, 0))                      as success_thread_num,
                count(distinct goods_id)                             as finished_goods_num,
                count(distinct if(result in (1, 6), goods_id, null)) as success_goods_num,
                sum(if(mt_diff = 1, add_uv, 0))                      as sum_add_uv_1m,
                sum(if(mt_diff = 1, detail_add_uv, 0))               as sum_detail_add_uv_1m,
                sum(if(mt_diff = 1, detail_view_uv, 0))              as sum_detail_view_uv_1m,
                sum(if(mt_diff = 1, sales_num, 0))                   as sum_sales_num_1m,
                sum(if(mt_diff = 1, sales_amount, 0))                as sum_sales_amount_1m,
                sum(if(mt_diff = 2, sales_amount, 0))                as sum_sales_amount_2m,
                sum(if(mt_diff = 3, sales_amount, 0))                as sum_sales_amount_3m
         from (
                  select test.project_name,
                         test.country_code,
                         test.platform_name,
                         test.cat_id,
                         test.cat_name,
                         ---
                         result,
                         test.goods_id,
                         floor(months_between(from_unixtime(unix_timestamp(gp.mt, 'yyyy-mm'), 'yyyy-mm-dd'),
                                              from_unixtime(unix_timestamp('${mt}', 'yyyy-mm'), 'yyyy-mm-dd'))) as mt_diff,
                         add_uv,
                         detail_add_uv,
                         detail_view_uv,
                         sales_num,
                         sales_amount
                  from dwd.dwd_fd_finished_goods_test test
                           left join dwd.dwd_fd_goods_performance_monthly gp
                                     on gp.mt between from_unixtime(unix_timestamp(add_months(from_unixtime(
                                                                                                      unix_timestamp('${mt}', 'yyyy-MM'),
                                                                                                      'yyyy-MM-dd HH:mm:ss'),
                                                                                              1),
                                                                                   'yyyy-MM-dd'), 'yyyy-MM')
                                            and from_unixtime(
                                                 unix_timestamp(
                                                         add_months(from_unixtime(unix_timestamp('${mt}', 'yyyy-MM'),
                                                                                  'yyyy-MM-dd HH:mm:ss'), 3),
                                                         'yyyy-MM-dd'), 'yyyy-MM')
                                         and test.result in (1, 6)
                                         and gp.virtual_goods_id = test.virtual_goods_id
                                         and gp.country_code = test.country_code
                                         and gp.platform_name = test.platform_name
                  where test.finish_time like '${mt}%') tmp
         group by project_name, country_code, platform_name, cat_id, cat_name) tmp2
         left join dwd.dwd_fd_category_sales_monthly csm1
                   on csm1.mt =
                      from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp('${mt}', 'yyyy-MM'),
                                                                            'yyyy-MM-dd HH:mm:ss'), 1),
                                                   'yyyy-MM-dd'), 'yyyy-MM')
                       and csm1.project_name = tmp2.project_name
                       and csm1.platform_name = tmp2.platform_name
                       and csm1.country_code = tmp2.country_code
                       and csm1.cat_id = tmp2.cat_id
         left join dwd.dwd_fd_category_sales_monthly csm2
                   on csm2.mt =
                      from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp('${mt}', 'yyyy-MM'),
                                                                            'yyyy-MM-dd HH:mm:ss'), 2),
                                                   'yyyy-MM-dd'), 'yyyy-MM')
                       and csm2.project_name = tmp2.project_name
                       and csm2.platform_name = tmp2.platform_name
                       and csm2.country_code = tmp2.country_code
                       and csm2.cat_id = tmp2.cat_id
         left join dwd.dwd_fd_category_sales_monthly csm3
                   on csm3.mt =
                      from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp('${mt}', 'yyyy-MM'),
                                                                            'yyyy-MM-dd HH:mm:ss'), 3),
                                                   'yyyy-MM-dd'), 'yyyy-MM')
                       and csm3.project_name = tmp2.project_name
                       and csm3.platform_name = tmp2.platform_name
                       and csm3.country_code = tmp2.country_code
                       and csm3.cat_id = tmp2.cat_id;