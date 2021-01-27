insert into table dwb.dwb_fd_erp_dispatch_link_report  partition (pt = '${pt}')
select
 /*+ REPARTITION(1) */
     concat(date_sub('${pt}', -1),' ${hour_str}') as record_time,
     t1.undelivered_goods_num as undelivered_goods_num,
     t4.due_dispatch_num as due_dispatch_num ,
     t4.no_dispatch_goods_num as no_dispatch_goods_num,
     t2.no_receive_dispatch_num as no_receive_dispatch_num,
     t3.no_qt_dispatch_num as no_qt_dispatch_num,
     t3.no_rk_dispatch_num as no_rk_dispatch_num,
     t2.no_sj_dispatch_num as no_sj_dispatch_num,
     t2.onlocing_dispatch_num as onlocing_dispatch_num,
     t3.on_loc_dispatch_num as on_loc_dispatch_num,
     t2.st_dispatch_num as st_dispatch_num,
     t1.pk_dispatch_num as pk_dispatch_num,
     t1.ck_dispatch_num as ck_dispatch_num,
     t3.ck_np_process_dispatch_num as ck_np_process_dispatch_num
from
    (SELECT '${pt}' as record_time,
    sum(if(not is_idle_stock,goods_num,0)) as undelivered_goods_num,
    count(if(pk and not on_loc,DISPATCH_LIST_ID,null)) as pk_dispatch_num,
    count(if(multi_ck or single_ck,DISPATCH_LIST_ID,null)) as ck_dispatch_num
    FROM dwd.dwd_fd_erp_order_dispatch_status_detail where pt='${pt}' ) t1
left join
    dwd.dwd_fd_erp_dispatch_link_goods_state_info t2 on t1.record_time=t2.record_time
left join
    dwd.dwd_fd_erp_dispatch_link_goods_stock_state_info t3 on t1.record_time=t3.record_time
left join
    dwd.dwd_fd_erp_dispatch_link_work_order_info t4 on t1.record_time=t4.record_time  ;

