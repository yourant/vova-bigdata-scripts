insert overwrite table dwb.dwb_fd_erp_daily_workload  partition (pt = '${pt}')
select
 /*+ REPARTITION(1) */
    t1.report_date as report_date ,
    t1.receive_dispatch_num as receive_dispatch_num,
    t1.qc_dispatch_num as qc_dispatch_num,
    t3.rk_dispatch_num as rk_dispatch_num,
    t1.sj_dispatch_num as sj_dispatch_num,
    t1.pk_dispatch_num as pk_dispatch_num,
    t3.ck_goods_num as ck_goods_num,
    t3.pack_goods_num as pack_goods_num,
    t2.deliver_order_num as deliver_order_num ,
    t2.deliver_goods_num as deliver_goods_num,
    t2.reserved_unck_single_order_num as reserved_unck_single_order_num,
    t2.reserved_unck_multi_order_num as reserved_unck_multi_order_num ,
    t3.package_num as package_num
from
dwd.dwd_fd_erp_daily_goods_handle_info t1
inner join
dwd.dwd_fd_erp_daily_order_goods_nums_info t2 on t1.report_date=t2.report_date
inner join
dwd.dwd_fd_erp_daily_stock_package_info  t3 on t1.report_date=t3.report_date
where t1.report_date='${pt}'
;
