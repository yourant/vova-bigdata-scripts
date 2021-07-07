sql="
select
month,
mct_id,
mct_name,
sum(gmv) gmv,
count(order_goods_id) confirm_order_cnt,
count(collect_order_goods_id) collect_order_cnt,
count(3pl_order_goods_id) 3pl_order_cnt,
count(comment_order_goods_id) comment_order_cnt,
count(mct_cancel_order_goods_id) mct_cancel_order_cnt,
count(mct_cancel_order_goods_id)/count(order_goods_id) mct_cancel_order_rate,
count(5_online_order_goods_id) 5_online_order_cnt,
count(3pl_5_online_order_goods_id) 3pl_5_online_order_cnt,
count(7_warehouse_order_goods_id) 7_warehouse_order_cnt,
avg(logistics_rating) avg_logistics_rating,
avg(rating) rating
from
(
select
trunc(og.confirm_time,'MM') month,
og.mct_id,
m.mct_name,
og.order_goods_id,
nvl(p.shop_price * p.goods_number + p.shipping_fee,0)  gmv,
case when l.collection_plan_id = 2 then og.order_goods_id end collect_order_goods_id,
case when l.collection_plan_id != 2 or l.collection_plan_id is null then og.order_goods_id end 3pl_order_goods_id,
case when c.order_goods_id is not null and trunc(og.confirm_time,'MM') = trunc(c.post_time,'MM') then og.order_goods_id end  comment_order_goods_id,
case when r.refund_type_id in (5,6,11) then og.order_goods_id end  mct_cancel_order_goods_id,
case when to_date(l.valid_tracking_date)>'2000-01-01' and datediff(l.valid_tracking_date,og.confirm_time)<5  and og.sku_pay_status>1 then og.order_goods_id end 5_online_order_goods_id,
case when to_date(l.valid_tracking_date)>'2000-01-01' and datediff(l.valid_tracking_date,og.confirm_time)<5  and og.sku_pay_status>1 and  l.collection_plan_id != 2  then og.order_goods_id end 3pl_5_online_order_goods_id,
case when to_date(cog.in_warehouse_time)>'2000-01-01' and datediff(cog.in_warehouse_time,og.confirm_time)<7  and l.collection_plan_id = 2  then og.order_goods_id end 7_warehouse_order_goods_id,
c.logistics_rating,
c.rating
from dim.dim_vova_order_goods og
left join dwd.dwd_vova_fact_pay p on og.order_goods_id = p.order_goods_id
left join dwd.dwd_vova_fact_comment c on og.order_goods_id = c.order_goods_id
left join dwd.dwd_vova_fact_refund r on og.order_goods_id = r.order_goods_id
left join dwd.dwd_vova_fact_logistics l on og.order_goods_id =l.order_goods_id
left join dim.dim_vova_merchant m on og.mct_id =m.mct_id
left join ods_vova_vts.ods_vova_collection_order_goods cog on og.order_goods_id = cog.order_goods_id
where og.confirm_time >='2021-02-01 00:00:00' and og.confirm_time <'2021-06-01 00:00:00'
and og.datasource ='vova'
) t group by month,mct_id,mct_name;



=======================================================================
select
month,
mct_id,
mct_name,
count(order_goods_id) confirm_order_cnt,
count(ship_order_goods_id) ship_order_cnt,
count(refund_order_goods_id) refund_order_cnt,
count(mct_audit_order_goods_id)mct_audit_order_cnt,
count(mct_audit_rejected_order_goods_id) mct_audit_rejected_order_cnt,
count(mct_audit_appeal_order_goods_id) mct_audit_appeal__order_cnt
from
(
select
case when to_date(og.confirm_time)>='2020-12-12' and to_date(og.confirm_time)<='2021-01-10' then '2020-12-12'
when to_date(og.confirm_time)>='2021-01-11' and to_date(og.confirm_time)<='2021-02-09' then '2021-01-11'
when to_date(og.confirm_time)>='2021-02-10' and to_date(og.confirm_time)<='2021-03-11' then '2021-02-10'
when to_date(og.confirm_time)>='2021-03-12' and to_date(og.confirm_time)<='2021-04-10' then '2021-03-12'
end month,
og.mct_id,
m.mct_name,
og.order_goods_id,
if(og.sku_shipping_status>0,og.order_goods_id,null) ship_order_goods_id,
if(fr.refund_type_id=2,og.order_goods_id,null) refund_order_goods_id,
case when fr.refund_type_id=2 and rr.mct_audit_num>0 then og.order_goods_id end mct_audit_order_goods_id,
case when fr.refund_type_id=2 and t.mct_audit_status='mct_audit_rejected' then og.order_goods_id end mct_audit_rejected_order_goods_id,
case when fr.refund_type_id=2 and rr.mct_audit_num>0 and t.mct_audit_status='mct_audit_rejected' and fr.recheck_type=2 then og.order_goods_id end mct_audit_appeal_order_goods_id
from dim.dim_vova_order_goods og
left join dwd.dwd_vova_fact_refund fr on og.order_goods_id = fr.order_goods_id
left join  ods_vova_vts.ods_vova_refund_reason rr on og.order_goods_id = rr.order_goods_id
left join (
select
order_goods_id,
mct_audit_status
from  ods_vova_vts.ods_vova_refund_audit_txn where mct_audit_status ='mct_audit_rejected' group by order_goods_id,mct_audit_status
) t on og.order_goods_id = t.order_goods_id
left join dim.dim_vova_merchant m on og.mct_id =m.mct_id
where og.confirm_time >='2020-12-12 00:00:00' and og.confirm_time <'2021-04-11 00:00:00'
and og.datasource ='vova'
) t group by month,mct_id,mct_name;

























"
