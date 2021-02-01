 insert overwrite table dwd.dwd_fd_erp_order_dispatch_status_detail partition (pt = '${pt}')

SELECT
oi.order_id,
og.rec_id,
oird.ORDER_INV_RESERVED_detail_ID,
nvl(oird.goods_number,0) AS goods_num,
nvl(oird.reserved_quantity, 0) AS reserved_num,
g.is_batch,
dl.DISPATCH_LIST_ID,
FALSE AS is_idle_stock,
dl.dispatch_sn,
dl.DISPATCH_STATUS_ID,
sdl.qc_status,
dl.due_date,
if(( qw.work_type = 'R'),TRUE ,FALSE) AS is_receive,
if((qw.work_type = 'S' ),TRUE ,FALSE) AS is_qt,

if((obdm.status = 'RK' AND obdm.is_process = 'Y' ),TRUE,FALSE) AS multi_rk,
if(( obdm.status = 'SJ' AND obdm.is_process = 'Y' ),TRUE,FALSE) AS sj,
if((dloc.location_seq_id is not null),TRUE ,FALSE) AS on_loc,
if(( bsd.status = 'PK' AND bsd.is_process = 'Y' ),TRUE  ,FALSE) AS pk,
if(( bsd.status = 'ST' AND bsd.is_process = 'Y' ),TRUE  ,FALSE) AS st,
if(( bsd.status = 'CK' AND bsd.is_process = 'Y' ),TRUE  ,FALSE) AS multi_ck,
if(( obdm.status = 'CK' AND obdm.is_process = 'Y' ) ,TRUE,FALSE) AS single_ck,

if(( bsd.status = 'CK' AND bsd.is_process = 'N' ),TRUE  ,FALSE) AS multi_ck_no_process,
if(( obdm.status = 'CK' AND obdm.is_process = 'N' ),TRUE,FALSE) AS single_ck_no_process


FROM ods_fd_ecshop.ods_fd_ecs_order_info oi
LEFT JOIN ods_fd_ecshop.ods_fd_ecs_order_goods og ON og.order_id = oi.order_id
LEFT JOIN ods_fd_ecshop.ods_fd_ecs_goods g ON g.goods_id = og.goods_id
LEFT JOIN ods_fd_romeo.ods_fd_dispatch_list dl ON dl.order_goods_id = og.rec_id AND dl.dispatch_status_id NOT IN ('CANCELLED')
LEFT JOIN ods_fd_mps.ods_fd_supplier_dispatch_list sdl ON dl.DISPATCH_LIST_ID = sdl.DISPATCH_LIST_ID

LEFT JOIN (
          select distinct order_item_id ,goods_number,ORDER_INV_RESERVED_detail_ID,reserved_quantity,
            row_number() over (partition by order_item_id order by VERSION ) rank
           from ods_fd_romeo.ods_fd_order_inv_reserved_detail ) oird
           ON og.rec_id = oird.order_item_id


INNER JOIN ods_fd_romeo.ods_fd_party_config pc on oi.party_id =pc.party_id
-- 关联质检新流程记录主表qc_workload，判断 R-待检验 S-鞋子和配件的检验' is_qt和is_receive的状态
left join ods_fd_mps.ods_fd_qc_workload qw on qw.dispatch_sn = dl.DISPATCH_SN
--关联记录框和工单的关系表obdm ，判断  multi_rk，sj，single_ck_no_process，single_ck的值
left join ods_fd_romeo.ods_fd_obdm obdm on obdm.dispatch_sn = dl.dispatch_sn
--关联工单库位表dispatch_location 判断是否在库位？--关联上就在库位
left join ods_fd_romeo.ods_fd_dispatch_location dloc on dl.DISPATCH_SN = dloc.dispatch_sn
--关联 框和发货单详细记录表，里面记录了拣货下架时的全部商品信息  判断st，pk，multi_ck，multi_ck_no_process
left join ods_fd_romeo.ods_fd_basket_shipment_detail bsd on bsd.dispatch_sn = dl.dispatch_sn


WHERE  pc.party_code = 2
and oird.rank=1
AND oi.facility_id = '383497303'
AND oi.order_time > date_sub('${pt}', 365)
AND oi.order_type_id ='SALE'
AND oi.shipping_status IN(0,8,9)
AND oi.order_status !='2'
AND (substr(oi.email,8)!='tetx.com' or oi.email in ('ytlu@tetx.com','ssqin@tetx.com','xdli@tetx.com'))
AND substr(oi.email,8)!='i9i8.com'

union all

SELECT
oi.order_id as order_id ,
 0 as  og_rec_id ,
oird.ORDER_INV_RESERVED_detail_ID as order_inv_reserved_detail_id ,
0 as  goods_num ,
0 as  reserved_num ,
NULL as is_batch ,
dl.DISPATCH_LIST_ID as dispatch_list_id,
TRUE AS is_idle_stock  ,
dl.dispatch_sn as dispatch_sn,
NULL as  dispatch_status_id ,
NULL as qc_status ,
NULL as  due_date ,
NULL as  is_receive ,
NULL as  is_qt ,
NULL as  multi_rk ,
NULL as  sj,
if((dloc.location_seq_id is not null),TRUE,FALSE) AS on_loc,
if(( bsd.status = 'PK' AND bsd.is_process = 'Y' ),TRUE,FALSE) AS pk,
if(( bsd.status = 'ST' AND bsd.is_process = 'Y' ),TRUE,FALSE) AS st,
if(( bsd.status = 'CK' AND bsd.is_process = 'Y' ),TRUE,FALSE) AS multi_ck,
if(( obdm.status = 'CK' AND obdm.is_process = 'Y'),TRUE,FALSE) AS single_ck,
if(( bsd.status = 'CK' AND bsd.is_process = 'N' ),TRUE,FALSE)AS multi_ck_no_process,
if(( obdm.status = 'CK' AND obdm.is_process = 'N') ,TRUE,FALSE) AS single_ck_no_process

from ods_fd_ecshop.ods_fd_ecs_order_info oi
INNER JOIN ods_fd_romeo.ods_fd_party_config pc on cast(oi.party_id as string)=cast(pc.party_id as string)
INNER JOIN ods_fd_romeo.ods_fd_order_inv_reserved oir ON oir.order_id = oi.order_id
INNER JOIN ods_fd_romeo.ods_fd_order_inv_reserved_detail oird ON oir.order_id = oird.ORDER_ID
LEFT JOIN ods_fd_romeo.ods_fd_order_inv_reserverd_inventory_mapping orim ON oird.ORDER_INV_RESERVED_DETAIL_ID = orim.ORDER_INV_RESERVED_DETAIL_ID
LEFT JOIN ods_fd_romeo.ods_fd_inventory_item ii ON orim.INVENTORY_ITEM_ID = ii.INVENTORY_ITEM_ID
LEFT JOIN ods_fd_romeo.ods_fd_inventory_item_detail iid ON ii.inventory_item_id = iid.INVENTORY_ITEM_ID
LEFT JOIN ods_fd_romeo.ods_fd_dispatch_list dl ON iid.order_id = dl.purchase_order_id AND dl.dispatch_status_id NOT IN ('CANCELLED')
LEFT JOIN ods_fd_mps.ods_fd_supplier_dispatch_list sdl ON dl.DISPATCH_LIST_ID = sdl.DISPATCH_LIST_ID
--关联工单库位表dispatch_location 判断是否在库位？--关联上就在库位
left join ods_fd_romeo.ods_fd_dispatch_location dloc on dl.DISPATCH_SN = dloc.dispatch_sn
--关联 框和发货单详细记录表，里面记录了拣货下架时的全部商品信息  判断st，pk，multi_ck，multi_ck_no_process
left join ods_fd_romeo.ods_fd_basket_shipment_detail bsd on bsd.dispatch_sn = dl.dispatch_sn
--关联记录框和工单的关系表obdm ，判断  multi_rk，sj，single_ck_no_process，single_ck的值
left join ods_fd_romeo.ods_fd_obdm obdm on obdm.dispatch_sn = dl.dispatch_sn

WHERE
 pc.party_code = 2
and  iid.QUANTITY_ON_HAND_DIFF > 0
AND oi.facility_id = '383497303'
AND oi.order_time > date_sub('${pt}',365)
AND oi.order_type_id ='SALE'
AND oi.shipping_status IN(0,8,9)
AND oi.order_status !='2'
AND (substr(oi.email,8)!='tetx.com' or oi.email in ('ytlu@tetx.com','ssqin@tetx.com','xdli@tetx.com'))
AND substr(oi.email,8)!='i9i8.com'
AND dl.ORDER_ID!=oi.order_id;







