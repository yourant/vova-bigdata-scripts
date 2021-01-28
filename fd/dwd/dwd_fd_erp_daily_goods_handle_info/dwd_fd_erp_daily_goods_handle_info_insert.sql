insert overwrite table dwd.dwd_fd_erp_daily_goods_handle_info  partition (pt = '${pt}')
select
    t1.report_date as report_date,
    t1.receive_dispatch_num as receive_dispatch_num,
    t2.qc_dispatch_num as qc_dispatch_num,
    t3.sj_dispatch_num as sj_dispatch_num,
    t4.pk_dispatch_num as pk_dispatch_num
from
-- 收货
     (    SELECT '${pt}' as report_date,
               count(*)  as receive_dispatch_num
        FROM ods_fd_mps.ods_fd_receipt_batch rb
                 INNER JOIN ods_fd_mps.ods_fd_receipt_dispatch_sn rdsn ON rb.receipt_batch_id = rdsn.receipt_batch_id
                 INNER JOIN ods_fd_romeo.ods_fd_dispatch_list dl ON dl.dispatch_sn = rdsn.dispatch_sn
                 INNER JOIN ods_fd_ecshop.ods_fd_ecs_order_info eoi ON eoi.order_id = dl.order_id
                 INNER JOIN ods_fd_ecshop.ods_fd_ecs_order_goods eog ON eog.rec_id = dl.order_goods_id
                 INNER JOIN ods_fd_ecshop.ods_fd_ecs_goods eg ON eg.goods_id = eog.goods_id
                 INNER JOIN ods_fd_ecshop.ods_fd_category_product_line cpl ON eg.external_cat_id = cpl.category_id
                 INNER JOIN ods_fd_ecshop.ods_fd_ecs_provider ep ON ep.provider_id = dl.provider_id
                 inner join ods_fd_mps.ods_fd_qc_workload qw on qw.dispatch_sn = dl.dispatch_sn and  qw.work_type = 'S'
                 INNER JOIN ods_fd_romeo.ods_fd_party_config pc  on dl.party_id=pc.party_id and pc.party_code = '2'
        WHERE
         --  eoi.pt>'2021-01-16'
              rb.batch_time >= '${pt}'
          AND rb.batch_time < date_sub('${pt}',-1)
          AND rb.batch_type = 'receive'
          AND (cpl.cat_id =2609 or  cpl.cat_id =2645))  t1
  -- 质检
     left join (
           SELECT '${pt}' as report_date,
               count(*) as qc_dispatch_num
        FROM (SELECT qw.dispatch_sn, qw.work_date, qw.work_name
              FROM ods_fd_mps.ods_fd_qc_workload qw
              INNER JOIN ods_fd_romeo.ods_fd_party_config pc  on qw.party_id=pc.party_id
              WHERE qw.work_date >= '${pt}'
                AND qw.work_date < date_sub('${pt}',-1)
                AND qw.work_type = 'S'
                and pc.party_code = '2'

              UNION

              SELECT qwh.dispatch_sn, qwh.work_date, qwh.work_name
              FROM ods_fd_mps.ods_fd_qc_workload_history qwh
              INNER JOIN ods_fd_romeo.ods_fd_party_config pc  on qwh.party_id=pc.party_id
                 WHERE qwh.work_date>= '${pt}'
                AND qwh.work_date < date_sub('${pt}',-1)
                AND qwh.work_type = 'S'
                 and pc.party_code = '2'
             ) as t
                 INNER JOIN ods_fd_romeo.ods_fd_dispatch_list dl
                            on dl.dispatch_sn = t.dispatch_sn
                 INNER JOIN ods_fd_ecshop.ods_fd_ecs_order_info eoi
                            on eoi.order_id = dl.order_id ) t2
     on t1.report_date=t2.report_date
 --   上架 --优化不需要关联facility
     left join (
       SELECT '${pt}' as report_date,
               count(*) as sj_dispatch_num
        FROM (
                 (
                     SELECT ro.abs_id, ro.create_time, ro.dispatch_sn
                     FROM ods_fd_romeo.ods_fd_obdm AS ro
                              INNER JOIN ods_fd_romeo.ods_fd_obcc AS oc ON ro.bar_code = oc.bar_code
                              INNER JOIN ods_fd_romeo.ods_fd_facility rf ON oc.facility_id = rf.facility_id
                              INNER JOIN ods_fd_romeo.ods_fd_dispatch_list AS dl ON ro.dispatch_sn = dl.dispatch_sn
                               INNER JOIN ods_fd_romeo.ods_fd_party_config pc on dl.party_id =pc.party_id
                     WHERE oc.facility_id ='383497303'
                       AND ro.is_fail = 'N'
                       AND ro.`STATUS` = 'SJ'
                       AND ro.is_process = 'N'
                       and pc.party_code = '2'
                       AND ro.create_time >= '${pt}'
                       AND ro.create_time < date_sub('${pt}',-1)
                 )
                 UNION
                 (
                     SELECT roh.abs_id, roh.create_time, roh.dispatch_sn
                     FROM ods_fd_romeo.ods_fd_obdm_history AS roh
                              INNER JOIN ods_fd_romeo.ods_fd_obcc oc ON oc.bar_code = roh.bar_code
                              INNER JOIN ods_fd_romeo.ods_fd_facility rf ON oc.facility_id = rf.facility_id
                              INNER JOIN ods_fd_romeo.ods_fd_dispatch_list AS dl ON roh.dispatch_sn = dl.dispatch_sn
                              INNER JOIN ods_fd_romeo.ods_fd_party_config pc on dl.party_id =pc.party_id
                     WHERE
                           roh.is_fail = 'N'
                       and oc.facility_id ='383497303'
                       AND roh.`STATUS` = 'SJ'
                       and pc.party_code = '2'
                       AND roh.create_time >= '${pt}'
                       AND roh.create_time < date_sub('${pt}',-1)
                 )) AS t ) t3
     on t1.report_date=t3.report_date

  -- 拣货
     left join (
        select
         '${pt}' as report_date,
         sum(eog.goods_number) as pk_dispatch_num from (
               (
                SELECT
                    os.order_id
                FROM ods_fd_romeo.ods_fd_basket_shipment_detail AS bsd
                INNER JOIN ods_fd_romeo.ods_fd_facility_location fl ON fl.location_seq_id = bsd.location_seq_id
                INNER JOIN ods_fd_romeo.ods_fd_facility rf ON fl.facility_id  = rf.facility_id
                INNER JOIN ods_fd_romeo.ods_fd_shipment rs ON rs.SHIPMENT_ID = bsd.shipment_id
                INNER JOIN ods_fd_romeo.ods_fd_party_config pc on rs.party_id= pc.party_id
                INNER JOIN ods_fd_romeo.ods_fd_order_shipment os on os.shipment_id = rs.shipment_id
                WHERE fl.facility_id ='383497303'
                      AND bsd.`status` = 'PK'
                      AND bsd.is_process = 'N'
                      AND bsd.created_stamp  >= '${pt}'
                      AND bsd.created_stamp  < date_sub('${pt}',-2)
                      and pc.party_code = '2'
                      group by os.order_id
                )
               UNION
                (
                 SELECT
                      os.order_id
                 FROM ods_fd_romeo.ods_fd_basket_shipment_detail_history AS bsdh
                 INNER JOIN ods_fd_romeo.ods_fd_facility_location fl ON fl.location_seq_id = bsdh.location_seq_id
                 INNER JOIN ods_fd_romeo.ods_fd_facility rf ON fl.facility_id = rf.facility_id
                 INNER JOIN ods_fd_romeo.ods_fd_shipment AS rs ON rs.SHIPMENT_ID = bsdh.shipment_id
                 INNER JOIN ods_fd_romeo.ods_fd_order_shipment os on os.shipment_id = rs.shipment_id
                 WHERE fl.facility_id ='383497303'
                       AND bsdh.status = 'PK'
                       AND bsdh.is_process = 'N'
                       AND bsdh.created_stamp  >= '${pt}'
                       AND bsdh.created_stamp  < date_sub('${pt}',-1)
                       group by os.order_id
                 )) t
         INNER JOIN ods_fd_ecshop.ods_fd_ecs_order_goods eog on eog.order_id = t.order_id
     ) t4
     on t1.report_date=t4.report_date
