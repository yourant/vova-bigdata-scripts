insert overwrite table dwd.dwd_fd_erp_dispatch_link_work_order_info  partition (pt = '${pt}')
select
t1.record_time as record_time,
t1.due_dispatch_num as due_dispatch_num,
t2.no_dispatch_goods_num as no_dispatch_goods_num

from
-- due_dispatch_num 工单超期数  ok
     (  SELECT
             '${pt}' as record_time,
             SUM(fpdr.overdue_quantity) AS due_dispatch_num
            FROM ods_fd_ecshop.ods_fd_fd_provider_daily_report AS fpdr
             INNER JOIN ods_fd_romeo.ods_fd_party_config pc on fpdr.party_id= pc.party_id
            WHERE
               fpdr.created_date = '${pt}'
              AND  pc.party_code = 2 ) t1
left join
-- no_dispatch_goods_num 未创建工单
      (  select
            '${pt}' as record_time,
             sum(t.num) as no_dispatch_goods_num
            from (select if(s.demand_quantity - s.available_to_reserved > 0, s.demand_quantity - s.available_to_reserved,0) as num
                  from
                  ( select goods_sn from ods_fd_romeo.ods_fd_dispatch_list where dispatch_status_id = 'PREPARE' GROUP BY goods_sn ) dl
                  INNER JOIN ods_fd_ecshop.ods_fd_ecs_goods eg on eg.uniq_sku = dl.goods_sn
                  INNER JOIN ods_fd_romeo.ods_fd_inventory_summary s on s.product_id = eg.product_id
                  where
                    s.facility_id = '383497303' ) t
                  ) t2
on t1.record_time=t2.record_time ;