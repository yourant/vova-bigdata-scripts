insert overwrite table dwd.dwd_fd_erp_dispatch_link_goods_stock_state_info partition (pt = '${pt}')
select
t1.record_time as record_time,
t1.no_qt_dispatch_num as no_qt_dispatch_num,
t1.no_rk_dispatch_num as no_rk_dispatch_num,
t2.on_loc_dispatch_num as on_loc_dispatch_num ,
t3.ck_np_process_dispatch_num as ck_np_process_dispatch_num

from
--`no_qt_dispatch_num`  '已收货未质检',
-- `no_rk_dispatch_num` '已质检未入库',
      ( select
                '${pt}' as record_time,
               -- # 已收货未质检
                count(if(DISPATCH_STATUS_ID = 'OK' and rs_type = 'R', DISPATCH_LIST_ID, null)) as no_qt_dispatch_num,
             --   # 已质检未入库
                count(if(DISPATCH_STATUS_ID = 'OK' and rs_type = 'S' and qc_status not in ('return', 'repair'),
                         DISPATCH_LIST_ID, null))  as no_rk_dispatch_num
            from (
                     SELECT
                            dl.DISPATCH_LIST_ID,
                            dl.DISPATCH_SN,
                            dl.DISPATCH_STATUS_ID,
                            sdl.qc_status
                     FROM ods_fd_romeo.ods_fd_dispatch_list dl
                              LEFT JOIN ods_fd_mps.ods_fd_supplier_dispatch_list sdl ON dl.DISPATCH_LIST_ID = sdl.DISPATCH_LIST_ID
                              LEFT JOIN ods_fd_ecshop.ods_fd_ecs_order_goods og ON dl.order_goods_id = og.rec_id
                              LEFT JOIN ods_fd_ecshop.ods_fd_ecs_order_info oi ON og.order_id = oi.order_id
                              INNER JOIN ods_fd_romeo.ods_fd_party_config pc on oi.party_id = pc.party_id
                     WHERE dl.DISPATCH_STATUS_ID = 'OK'
                       AND dl.SUBMIT_DATE > date_sub('${pt}', 365)
                       AND pc.party_code = 2
                       AND oi.facility_id = '383497303'
                       AND oi.order_time > date_sub('${pt}', 365)
                     -- 原来的注释了
                     --  # AND oi.order_type_id = 'SALE'
                       AND oi.order_status != '2'
                       AND (substr(oi.email, 8) != 'tetx.com' or
                            oi.email in ('ytlu@tetx.com', 'ssqin@tetx.com', 'xdli@tetx.com'))
                       AND substr(oi.email, 8) != 'i9i8.com'
                 ) t1
                 left join
                 -- 选择时间最新的状态R或者S
                 (
                 select
                 qw.dispatch_sn  ,
                 qw.work_type as rs_type ,
                 row_number() over(partition by qw.dispatch_sn order by qw.work_date desc ) as rank
                                    from ods_fd_mps.ods_fd_qc_workload qw
                                    inner join ods_fd_romeo.ods_fd_dispatch_list dl
                                    on qw.dispatch_sn = dl.DISPATCH_SN
                                    where qw.work_type in ('R', 'S')
                 ) t2
                 on t1.DISPATCH_SN=t2.dispatch_sn
                 where t2.rank=1 ) t1

left join
--  `on_loc_dispatch_num` '在库位' ？
(select
      '${pt}' as record_time,
      t1.on_loc_dispatch_num+t2.on_loc_dispatch_num as on_loc_dispatch_num
     from (
            select
            'join_id' as id ,
            count(*) as on_loc_dispatch_num
            from ods_fd_romeo.ods_fd_basket_shipment s
            INNER JOIN ods_fd_romeo.ods_fd_basket_shipment_detail d
            on d.shipment_id = s.shipment_id
            where s.status = 'PK' and s.is_process = 'Y'
       ) t1
       left join
    (
            select
             'join_id' as id ,
            count(*) as on_loc_dispatch_num
            from ods_fd_romeo.ods_fd_order_inv_reserved oir
                     INNER JOIN ods_fd_romeo.ods_fd_order_inv_reserved_detail oird
                                on oird.order_id = oir.order_id
                     INNER JOIN ods_fd_romeo.ods_fd_order_inv_reserverd_inventory_mapping m
                                on m.order_inv_reserved_detail_id = oird.order_inv_reserved_detail_id
                     INNER JOIN ods_fd_romeo.ods_fd_inventory_item ii
                                on ii.inventory_item_id = m.inventory_item_id
                     INNER JOIN ods_fd_romeo.ods_fd_inventory_item_detail iid
                                on iid.inventory_item_id = ii.root_inventory_item_id
                     INNER JOIN ods_fd_romeo.ods_fd_dispatch_list dl
                                on dl.purchase_order_id = iid.order_id
                     INNER JOIN ods_fd_romeo.ods_fd_dispatch_location l
                                on l.dispatch_sn = dl.dispatch_sn
            where
              iid.quantity_on_hand_diff > 0
              and oir.status = 'Y'
              and oir.facility_id = '383497303'
        ) t2
        on t1.id=t2.id ) t2

on t1.record_time=t2.record_time

left join

-- `ck_np_process_dispatch_num` '已出库未发货'  OK
   (        select
            '${pt}' as record_time,
            sum(eog.goods_number) as ck_np_process_dispatch_num
            from ods_fd_ecshop.ods_fd_ecs_order_info eoi
                     INNER JOIN ods_fd_ecshop.ods_fd_ecs_order_goods eog
                                on eog.order_id = eoi.order_id
            where eoi.shipping_status = '8'
              and lower(eoi.order_type_id) = 'sale'
              and eoi.facility_id = '383497303' ) t3
on t1.record_time=t3.record_time ;