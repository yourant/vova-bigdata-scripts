sql="
select
og.mct_id,
count(og.order_goods_id) confirm_cnt,
count(if(og.sku_shipping_status>0,og.order_goods_id,null)) shipping_cnt,
count(if(fr.refund_type_id=2,og.order_goods_id,null)) refund_cnt,
count(if(fr.refund_type_id=2 and og.sku_pay_status in (3,4),og.order_goods_id,null)) refund_ok_cnt,
count(if(fr.refund_type_id=2 and fr.recheck_type=2,og.order_goods_id,null)) refund_appeal_cnt,
count(if(fr.refund_type_id=2 and fr.refund_reason_type_id in (8,11,12) and og.sku_pay_status in (3,4),og.order_goods_id,null)) logic_refund_ok_cnt,
count(if(fr.refund_type_id=2 and fr.refund_reason_type_id not in (8,11,12) and og.sku_pay_status in (3,4),og.order_goods_id,null)) no_logic_refund_ok_cnt,
count(if(fr.refund_type_id=2 and fr.refund_reason_type_id in (8,11,12) and fr.recheck_type=2,og.order_goods_id,null)) logic_refund_appeal_cnt,
count(if(fr.refund_type_id=2 and fr.refund_reason_type_id not in (8,11,12) and fr.recheck_type=2,og.order_goods_id,null)) no_logic_refund_appeal_cnt,
count(if(fr.refund_type_id=12,og.order_goods_id,null)) cb_cnt,
count(if(fr.refund_type_id=2 and rr.mct_audit_num!=0,og.order_goods_id,null)) mct_audit_cnt,
count(if(fr.refund_type_id=2 and t.mct_audit_status='mct_audit_rejected',og.order_goods_id,null)) mct_reject_cnt,
count(if(fr.refund_type_id=2 and fr.recheck_type=2 and t.mct_audit_status='mct_audit_rejected',og.order_goods_id,null)) mct_reject_appeal_cnt
from dim.dim_vova_order_goods og
left join dwd.dwd_vova_fact_refund fr on og.order_goods_id = fr.order_goods_id
left join  ods_vova_vts.ods_vova_refund_reason rr on og.order_goods_id = rr.order_goods_id
left join (
select
order_goods_id,
mct_audit_status
from  ods_vova_vts.ods_vova_refund_audit_txn where mct_audit_status ='mct_audit_rejected' group by order_goods_id,mct_audit_status
) t on og.order_goods_id = t.order_goods_id
where to_date(og.confirm_time)>='2021-01-10' and to_date(og.confirm_time)<='2021-01-30'
group by og.mct_id limit 10



"