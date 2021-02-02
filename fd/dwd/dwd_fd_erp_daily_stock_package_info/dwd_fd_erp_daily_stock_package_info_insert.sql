
insert overwrite table dwd.dwd_fd_erp_daily_stock_package_info  partition (pt = '${pt}')
select
t1.report_date as report_date,
t1.package_num as package_num,
t2.pack_goods_num as pack_goods_num ,
t3.ck_goods_num as ck_goods_num,
t4.rk_dispatch_num as rk_dispatch_num
from
-- 打包数
 (SELECT '${pt}' as report_date,
         count(*) as package_num
     FROM ods_fd_ecshop.ods_fd_package_workload_statistics pws
     INNER JOIN ods_fd_romeo.ods_fd_order_shipment os on os.SHIPMENT_ID = pws.shipment_id
     INNER JOIN ods_fd_ecshop.ods_fd_ecs_order_info eoi on eoi.order_id  =os.ORDER_ID
     INNER JOIN ods_fd_romeo.ods_fd_party_config pc on eoi.party_id  =pc.party_id

              WHERE pws.created_time  >= '${pt}'
                AND pws.created_time  < date_sub('${pt}',-1)
                AND pws.status = 'F'
                AND eoi.facility_id ='383497303'
                and pc.party_code = '2'
              )  t1
 left join

  -- 打包商品数 订单号去重后再统计
 (
        SELECT '${pt}'as report_date,
        sum(cast(eog1.goods_number as int))  as pack_goods_num
        FROM (
         SELECT
                     eoi.order_id as order_id
              FROM ods_fd_ecshop.ods_fd_package_workload_statistics pws
                       INNER JOIN ods_fd_romeo.ods_fd_order_shipment os on os.SHIPMENT_ID = pws.shipment_id
                       INNER JOIN ods_fd_ecshop.ods_fd_ecs_order_info eoi on cast(eoi.order_id as string) = os.ORDER_ID
                       INNER JOIN ods_fd_romeo.ods_fd_party_config pc on cast(eoi.party_id as string) =pc.party_id
                       INNER JOIN ods_fd_ecshop.ods_fd_ecs_order_goods eog on eog.order_Id = eoi.order_id
              WHERE
                eoi.facility_id ='383497303'
                 and pc.party_code = '2'
                AND pws.created_time  >= '${pt}'
                AND pws.created_time < date_sub('${pt}',-1)
                AND pws.status = 'F'
              group by eoi.order_id
              ) t
                 INNER JOIN ods_fd_ecshop.ods_fd_ecs_order_goods eog1 on eog1.order_id = t.order_id ) t2

 on t1.report_date=t2.report_date


  -- 出库
    left join (
     SELECT '${pt}' as report_date,
               count(*)  as ck_goods_num
        FROM (
             (SELECT bsd.dispatch_sn, bsd.created_stamp as time, bsd.abs_id as id
               FROM ods_fd_romeo.ods_fd_basket_shipment_detail bsd
                        INNER JOIN ods_fd_romeo.ods_fd_facility_location fl ON fl.location_seq_id = bsd.location_seq_id
                        INNER JOIN ods_fd_romeo.ods_fd_facility f ON fl.facility_id  = f.facility_id
                        INNER JOIN ods_fd_romeo.ods_fd_shipment rs ON rs.SHIPMENT_ID = bsd.shipment_id
                        INNER JOIN ods_fd_romeo.ods_fd_party_config pc on rs.party_id =pc.party_id
               WHERE
                fl.facility_id ='383497303'
                 and pc.party_code = '2'
                 AND bsd.status = 'CK'
                 AND bsd.is_process = 'N'
                 AND bsd.created_stamp  >= '${pt}'
                 AND bsd.created_stamp < date_sub('${pt}',-1)
              )
              UNION
              (
                  SELECT bsd.dispatch_sn, bsd.created_stamp as time, bsd.abs_id as id
                  FROM ods_fd_romeo.ods_fd_basket_shipment_detail_history bsd
                           INNER JOIN ods_fd_romeo.ods_fd_facility_location fl ON fl.location_seq_id = bsd.location_seq_id
                           INNER JOIN ods_fd_romeo.ods_fd_facility f ON fl.facility_id  = f.facility_id
                           INNER JOIN ods_fd_romeo.ods_fd_shipment rs ON rs.SHIPMENT_ID = bsd.shipment_id
                           INNER JOIN ods_fd_romeo.ods_fd_party_config pc on rs.party_id =pc.party_id

                  WHERE fl.facility_id ='383497303'
                    and pc.party_code = '2'
                    AND bsd.status = 'CK'
                    AND bsd.is_process = 'N'
                AND bsd.created_stamp  >= '${pt}'
                 AND bsd.created_stamp< date_sub('${pt}',-1)
              )
              UNION
              (
                  SELECT dl.dispatch_sn, ro.create_time as time, ro.abs_id as id
                  FROM ods_fd_romeo.ods_fd_obdm AS ro
                           INNER JOIN ods_fd_romeo.ods_fd_dispatch_list dl ON dl.dispatch_sn = ro.dispatch_sn
                           INNER JOIN ods_fd_romeo.ods_fd_party_config pc on dl.party_id=pc.party_id
                           INNER JOIN ods_fd_romeo.ods_fd_inventory_item_detail iid on iid.order_id = dl.purchase_order_id
                           INNER JOIN ods_fd_romeo.ods_fd_inventory_item ii on ii.inventory_item_id = iid.inventory_item_id
                           INNER JOIN ods_fd_romeo.ods_fd_facility f on f.facility_id = ii.facility_id
                           INNER JOIN ods_fd_romeo.ods_fd_order_inv_reserverd_inventory_mapping m on m.inventory_item_id = ii.inventory_item_id
                           INNER JOIN ods_fd_romeo.ods_fd_order_inv_reserved_detail oird on oird.order_inv_reserved_detail_id = m.order_inv_reserved_detail_id
                           INNER JOIN ods_fd_romeo.ods_fd_order_shipment os on os.order_id = oird.order_id
                  WHERE
                        pc.party_code = '2'
                    AND iid.quantity_on_hAND_diff > 0
                    and ii.status_id = 'INV_STTS_AVAILABLE'
                    and ii.facility_id ='383497303'
                    AND ro.is_fail = 'N'
                    AND ro.status = 'CK'
                    AND ro.is_process = 'N'
                    AND ro.create_time >= '${pt}'
                    AND ro.create_time < date_sub('${pt}',-1)
              )
              UNION
              (
                  SELECT dl.dispatch_sn, ro.create_time as time, ro.abs_id
                  FROM ods_fd_romeo.ods_fd_obdm_history AS ro
                           INNER JOIN ods_fd_romeo.ods_fd_dispatch_list AS dl ON dl.dispatch_sn = ro.dispatch_sn
                           INNER JOIN ods_fd_romeo.ods_fd_party_config pc on dl.party_id=pc.party_id
                           INNER JOIN ods_fd_romeo.ods_fd_inventory_item_detail iid
                                      on iid.order_id = dl.purchase_order_id
                           INNER JOIN ods_fd_romeo.ods_fd_inventory_item ii on ii.inventory_item_id = iid.inventory_item_id
                           INNER JOIN ods_fd_romeo.ods_fd_facility f on f.facility_id = ii.facility_id
                           INNER JOIN ods_fd_romeo.ods_fd_order_inv_reserverd_inventory_mapping m
                                      on m.inventory_item_id = ii.inventory_item_id
                           INNER JOIN ods_fd_romeo.ods_fd_order_inv_reserved_detail oird
                                      on oird.order_inv_reserved_detail_id = m.order_inv_reserved_detail_id
                           INNER JOIN ods_fd_romeo.ods_fd_order_shipment os on os.order_id = oird.order_id
                  WHERE
                    pc.party_code = '2'
                    AND iid.quantity_on_hAND_diff > 0
                    AND ro.is_fail = 'N'
                    AND ro.status = 'CK'
                    and ii.status_id = 'INV_STTS_AVAILABLE'
                    and ii.facility_id ='383497303'
                    AND ro.create_time >= '${pt}'
                    AND ro.create_time  < date_sub('${pt}',-1)
              )) as t
    ) t3
    on t1.report_date=t3.report_date
    left join

 -- -- 入库
 (
 SELECT '${pt}' as report_date,
               count(*)   as rk_dispatch_num
        FROM (
                 (
                     SELECT ro.abs_id, ro.create_time, ro.dispatch_sn
                     FROM ods_fd_romeo.ods_fd_obdm AS ro
                              INNER JOIN ods_fd_romeo.ods_fd_obcc AS oc ON ro.bar_code = oc.bar_code
                              INNER JOIN ods_fd_romeo.ods_fd_facility rf ON oc.facility_id = rf.facility_id
                              INNER JOIN ods_fd_romeo.ods_fd_dispatch_list AS dl ON ro.dispatch_sn = dl.dispatch_sn
                              INNER JOIN ods_fd_romeo.ods_fd_party_config pc on dl.party_id=pc.party_id
                     WHERE
                           oc.facility_id ='383497303'
                       AND ro.is_fail = 'N'
                       AND ro.STATUS = 'RK'
                       AND ro.is_process = 'N'
                       and pc.party_code = '2'
                       AND ro.create_time >= '${pt}'
                       AND ro.create_time  < date_sub('${pt}',-1)
                 )
                 UNION
                 (
                     SELECT roh.abs_id, roh.create_time, roh.dispatch_sn
                     FROM ods_fd_romeo.ods_fd_obdm_history AS roh
                              INNER JOIN ods_fd_romeo.ods_fd_obcc oc ON oc.bar_code = roh.bar_code
                              INNER JOIN ods_fd_romeo.ods_fd_facility rf ON oc.facility_id = rf.facility_id
                              INNER JOIN ods_fd_romeo.ods_fd_dispatch_list AS dl ON roh.dispatch_sn = dl.dispatch_sn
                              INNER JOIN ods_fd_romeo.ods_fd_party_config pc on dl.party_id =pc.party_id
                     WHERE roh.is_fail = 'N'
                       AND oc.facility_id ='383497303'
                       AND roh.STATUS = 'RK'
                       and pc.party_code = '2'
                       AND roh.create_time >= '${pt}'
                       AND roh.create_time < date_sub('${pt}',-1)
                 )) AS t ) t4
    on t1.report_date=t4.report_date ;
