insert overwrite table dwd.dwd_fd_erp_dispatch_link_goods_state_info partition (pt = '${pt}')
select
      t1.record_time as record_time ,
      t3.no_receive_dispatch_num as no_receive_dispatch_num,
      t1.no_sj_dispatch_num as no_sj_dispatch_num,
      t1.onlocing_dispatch_num as onlocing_dispatch_num ,
      t2.st_dispatch_num as st_dispatch_num

from
-- record_time, no_sj_dispatch_num 入库中, onlocing_dispatch_num 上架中
 (select
            '${pt}' as record_time,
            sum(if(multi_rk and not sj, 1, 0)) as no_sj_dispatch_num,
            sum(if(sj and not on_loc, 1, 0))   as onlocing_dispatch_num
            from (
  SELECT
    dl.DISPATCH_LIST_ID,
    dl.DISPATCH_SN,
    if(( obdm.status = 'RK'AND is_process = 'Y'),TRUE, FALSE) AS multi_rk,
    if(( obdm.status = 'SJ'AND is_process = 'Y'),TRUE ,FALSE) AS sj,
    if((dloc.location_seq_id is not null),TRUE ,FALSE) as on_loc

    FROM ods_fd_romeo.ods_fd_dispatch_list dl
    LEFT JOIN ods_fd_romeo.ods_fd_inventory_item_detail iid ON dl.purchase_order_id  = iid.order_id
    LEFT JOIN ods_fd_romeo.ods_fd_inventory_item ii ON iid.inventory_item_id = ii.inventory_item_id
    INNER JOIN ods_fd_romeo.ods_fd_party_config pc on dl.party_id=pc.party_id
--关联记录框和工单的关系表obdm ，判断  multi_rk，sj，single_ck_no_process，single_ck的值
left join ods_fd_romeo.ods_fd_obdm obdm on obdm.dispatch_sn = dl.dispatch_sn
--关联工单库位表dispatch_location 判断是否在库位？--关联上就在库位
left join ods_fd_romeo.ods_fd_dispatch_location dloc on dl.DISPATCH_SN = dloc.dispatch_sn
      WHERE
      iid.QUANTITY_ON_HAND_DIFF > 0
      AND iid.PHYSICAL_INVENTORY_ID IS NULL
      AND ii.STATUS_id = 'INV_STTS_AVAILABLE'
      AND  ii.QUANTITY_ON_HAND > 0
      and pc.party_code = 2
      AND dl.SUBMIT_DATE > date_sub('${pt}', 365)
      AND ii.facility_id = '383497303') as t ) t1

left join
-- st_dispatch_num 拣货下架中 ok
 ( select
             '${pt}' as record_time,
             count(*) as st_dispatch_num
            from ods_fd_romeo.ods_fd_basket_shipment s
            INNER JOIN ods_fd_romeo.ods_fd_basket_shipment_detail d
            on d.shipment_id = s.shipment_id
            where s.status = 'PK' and s.is_process = 'Y' ) t2
on t1.record_time=t2.record_time

-- no_receive_dispatch_num '已分配未收货', ok
left join (
            select
            '${pt}' as record_time,
            sum(t1.num) - sum(if(t1.num - num1 > 0, t1.num - num1, 0)) as no_receive_dispatch_num
            from (
            select
            'join_id' as id ,
            if(s.demand_quantity - s.available_to_reserved > 0, s.demand_quantity - s.available_to_reserved,0) as num
                  from ods_fd_romeo.ods_fd_inventory_summary s
                  INNER JOIN ods_fd_ecshop.ods_fd_ecs_goods eg on eg.product_id = s.PRODUCT_ID
                  INNER JOIN ods_fd_romeo.ods_fd_goods_purchase_price gpp on gpp.goods_id = eg.external_goods_id
                  where
                     s.FACILITY_ID = '383497303'
                    and s.STATUS_ID = 'INV_STTS_AVAILABLE'
         ) as t1
         left join (

          select
            'join_id' as id ,
             count(*) as num1
             from ods_fd_romeo.ods_fd_dispatch_list dl
             INNER JOIN ods_fd_mps.ods_fd_supplier_dispatch_list sdl on sdl.dispatch_list_id = dl.dispatch_list_id
             inner join  ods_fd_ecshop.ods_fd_ecs_goods eg on  dl.goods_sn = eg.uniq_sku
             where dl.dispatch_status_id = 'OK'
             and  sdl.qc_status in ('', 'return')
         ) as t2
         on t1.id =t2.id ) t3

  on t1.record_time=t3.record_time ;