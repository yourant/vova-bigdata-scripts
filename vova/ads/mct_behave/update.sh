#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
with tmp_mct_behave(
select
mct_id,
mct_name,
nvl(count(*),0) as confirm_order_cnt_3m,
nvl(count(if(day_gap<30,order_goods_id,null)),0) as confirm_order_cnt_1m,
nvl(count(if(day_gap<30 and is_pay =1,order_goods_id,null)),0) as order_cnt_1m,
nvl(sum(if(day_gap<30,total_amount,0)),0) as gmv_30d,
nvl(cast(sum(if(is_refund=1 and day_gap>=63,1,0 ))/sum(if(day_gap>=63,1,0 ))*100 as decimal(13,4)),0) as refund_rate_9w,
nvl(cast(sum(if(is_wl_refund=1 and day_gap>=63,1,0 ))/sum(if(day_gap>=63,1,0 ))*100 as decimal(13,4)),0) as wl_refund_rate_9w,
nvl(cast(sum(if(is_nwl_refund=1 and day_gap>=63,1,0 ))/sum(if(day_gap>=63,1,0 ))*100 as decimal(13,4)),0) as nwl_refund_rate_9w,
nvl(sum(if(is_mct_cancel=1 and day_gap>=8 and day_gap<37,1,0 )),0) as mct_cancel_cnt,
nvl(cast(sum(if(is_mct_cancel=1 and day_gap>=8 and day_gap<37,1,0 ))/sum(if(day_gap>=8 and day_gap<37,1,0 ))*100 as decimal(13,4)),0) as mct_cancel_rate,
nvl(cast(sum(if(is_mark_deliver=1 and day_gap>=6 and day_gap<7,1,0 ))/sum(if(day_gap>=6 and day_gap<7,1,0 ))*100 as decimal(13,4)),0) as mark_deliver_rate,
nvl(cast(sum(if(is_online=1 and day_gap>=8 and day_gap<37,1,0 ))/sum(if(day_gap>=8 and day_gap<37,1,0 ))*100 as decimal(13,4)),0) as online_rate,
nvl(cast(sum(if(is_loss_weight=1 and day_gap>=8 and day_gap<37,1,0 ))/sum(if(day_gap>=8 and day_gap<37,1,0 ))*100 as decimal(13,4)),0) as loss_weight_rate
from
(select
dm.mct_id,
dm.mct_name,
og.order_goods_id,
if(fp.order_goods_id is null ,0,1) as is_pay,
datediff('${pre_date}',og.confirm_time) as day_gap,
fp.shop_price*fp.goods_number+fp.shipping_fee as total_amount,
if(dr.refund_type_id in (2,12),1,0) is_refund,
if(dr.refund_type_id = 2 and dr.refund_reason_type_id=8 ,1,0) is_wl_refund,
if(dr.refund_type_id = 2 and dr.refund_reason_type_id!=8 ,1,0) is_nwl_refund,
if(dr.refund_type_id in (5,6,11,14) or og.sku_order_status=5,1,0) is_mct_cancel,
if(og.sku_shipping_status>=1,1,0) is_mark_deliver,
if(og.sku_pay_status>1 and og.sku_shipping_status > 0,1,0) is_online,
if(ostd.weight=0 or ostd.weight is null ,1,0) is_loss_weight
from
dim.dim_vova_merchant dm
left join dim.dim_vova_goods dg on dg.mct_id = dm.mct_id
left join dim.dim_vova_order_goods og on dg.goods_id = og.goods_id and datediff('${pre_date}',og.confirm_time)<90 and  datediff('${pre_date}',og.confirm_time)>=0
left join dwd.dwd_vova_fact_refund dr
on og.order_goods_id = dr.order_goods_id
left join dwd.dwd_vova_fact_pay fp
on og.order_goods_id = fp.order_goods_id
left join ods_vova_vts.ods_vova_order_shipping_tracking ost on ost.order_goods_id = og.order_goods_id
left join ods_vova_vts.ods_vova_order_shipping_tracking_detail ostd on ost.tracking_id = ostd.tracking_id
)
group by mct_id,mct_name
)
INSERT overwrite TABLE ads.ads_mct_behave_3m partition(pt='${pre_date}')
select
t1.*,
t2.expre_cnt/t1.order_cnt_1m as exp_income,
t3.second_cat_ids
from
tmp_mct_behave t1
left join
(select
dg.mct_id,
sum(expre_cnt) as expre_cnt
from
dws.dws_vova_buyer_goods_behave  gb
inner join dim.dim_vova_goods dg on gb.gs_id = dg.goods_id
where datediff('${pre_date}',pt)<30 and   datediff('${pre_date}',pt)>=0
group by dg.mct_id) t2
on t1.mct_id = t2.mct_id
left join (
select
mct_id,
concat_ws(',',collect_set(second_cat_id)) second_cat_ids
from
(select
mct_id,
second_cat_id,
row_number() over(partition by mct_id order by gmv desc) rank
from
(select
dg.mct_id,
dg.second_cat_id,
sum(fp.shop_price*fp.goods_number+fp.shipping_fee) gmv
from
dwd.dwd_vova_fact_pay fp
left join dim.dim_vova_goods dg
on fp.goods_id = dg.goods_id
where datediff('${pre_date}',fp.confirm_time)<30 and  datediff('${pre_date}',fp.confirm_time)>=0
group by
dg.mct_id,
dg.second_cat_id))
where rank<=3
group by mct_id
) t3
on t1.mct_id = t3.mct_id
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_mct_behave_3m" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
