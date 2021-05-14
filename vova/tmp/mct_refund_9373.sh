sql=
"
select
t.mct_id,
m.mct_name,
t1.max_rank,
confirm_cnt,
shipping_cnt,
refund_cnt,
refund_ok_cnt,
refund_appeal_cnt,
nvl(refund_ok_cnt/shipping_cnt,0) refund_rate,
logic_refund_ok_cnt,
nvl(logic_refund_ok_cnt/shipping_cnt,0) logic_refund_ok_rate,
no_logic_refund_ok_cnt,
nvl(no_logic_refund_ok_cnt/shipping_cnt,0) no_logic_refund_ok_rate,
nvl(refund_appeal_cnt/shipping_cnt,0) refund_appeal_rate,
logic_refund_appeal_cnt,
nvl(logic_refund_appeal_cnt/shipping_cnt,0) logic_refund_appeal_rate,
no_logic_refund_appeal_cnt,
nvl(no_logic_refund_appeal_cnt/shipping_cnt,0) no_logic_refund_appeal_rate,
cb_cnt,
nvl((cb_cnt+refund_appeal_cnt)/shipping_cnt,0) loss_rate,
mct_audit_cnt,
mct_reject_cnt,
mct_reject_appeal_cnt
from
(
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
group by og.mct_id
) t
left join
(
select
mct_id,
max(rank) max_rank
from ads.ads_vova_mct_rank where pt='2021-05-10' group by mct_id
) t1  on t.mct_id = t1.mct_id
left join dim.dim_vova_merchant m on t.mct_id =m.mct_id
"