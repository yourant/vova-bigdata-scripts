#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天     有用
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

sql="
set hive.strict.checks.type.safety=false;
set hive.mapred.mode=nostrict;

insert overwrite table dwb.dwb_vova_main_report partition(pt='${cur_date}')
select '${cur_date}' event_date,tmp1.mct_id,tmp1.mct_name,nvl(tmp1.imp_uvs,0) imp_uvs,nvl(tmp11.gmv,0)gmv ,nvl(tmp11.order_nums,0) order_nums,nvl(tmp11.buyer_nums,0) buyer_nums,nvl(tmp2.inac_gmv,0)inac_gmv,nvl(tmp2.inac_order_nums,0) inac_order_nums,nvl(tmp2.inac_buyer_nums,0)inac_buyer_nums,
nvl(tmp4.on_sale_gs,0) on_sale_gs,
nvl(tmp4.pdbuy_uv,0) pdbuy_uv,nvl(tmp4.pd_uv,0) pd_uv,nvl(tmp4.cart_success_uv,0)cart_success_uv ,nvl(tmp4.cart_uv,0) cart_uv,nvl(tmp5.on_sale_dt,'NA') on_sale_dt,nvl(tmp5.on_sale_dt,'NA') on_sale_dt2,nvl(tmp111.imp_pvs,0) imp_pvs,nvl(tmp2.spsor_name,'NA') spsor_name
from
(select p1.mct_id,p1.mct_name,count(distinct p2.device_id) imp_uvs
from dim.dim_vova_merchant p1
left join dim.dim_vova_goods  p0
on p1.mct_id=p0.mct_id
left join (select device_id,virtual_goods_id from dwd.dwd_vova_log_goods_impression where pt='${cur_date}')p2
on p0.virtual_goods_id=p2.virtual_goods_id
group by p1.mct_id,p1.mct_name
)tmp1
left join
(
select p3.mct_id,count(1) imp_pvs
from (select device_id,virtual_goods_id from dwd.dwd_vova_log_goods_impression where pt='${cur_date}')p1
left join dim.dim_vova_goods p2
on p1.virtual_goods_id=p2.virtual_goods_id
left join dim.dim_vova_merchant p3
on p2.mct_id=p3.mct_id
group by p3.mct_id
)tmp111
on tmp1.mct_id=tmp111.mct_id
left join
(
select mct_id,sum(shop_price * nvl(goods_number,0) + nvl(shipping_fee,0)) gmv,count(distinct order_id) order_nums,count(distinct buyer_id) buyer_nums from dwd.dwd_vova_fact_pay  where to_date(pay_time)='${cur_date}' group by mct_id
)tmp11
on tmp1.mct_id=tmp11.mct_id

left join
(
select p1.mct_id,p1.spsor_name,p3.inac_gmv,p3.inac_order_nums,p3.inac_buyer_nums
from dim.dim_vova_merchant p1
left join (select fp.mct_id,sum(fp.shop_price * nvl(fp.goods_number,0) + nvl(fp.shipping_fee,0)) inac_gmv, count(distinct fp.order_id) inac_order_nums, count(distinct fp.buyer_id) inac_buyer_nums  from dwd.dwd_vova_fact_pay fp inner join  dim.dim_vova_order_goods og on og.order_goods_id = fp.order_goods_id where to_date(fp.pay_time)='${cur_date}' and og.order_tag is null group by fp.mct_id)p3
on p1.mct_id=p3.mct_id
)tmp2
on tmp1.mct_id=tmp2.mct_id

left join

(select tmp3.mct_id,count(distinct tmp3.virtual_goods_id) on_sale_gs,count(distinct tmp3.pdbuy_uv_dev) pdbuy_uv,count(distinct tmp3.pd_uv_dev) pd_uv,count(distinct tmp3.cart_success_uv_dev) cart_success_uv,
count(tmp3.cart_success_uv_dev) cart_uv from
(
select t1.mct_id,t2.virtual_goods_id,
CASE WHEN t3.event_type='pdbuy_uv' THEN t3.device_id end pdbuy_uv_dev,
CASE WHEN t3.event_type='pd_uv'   THEN t3.device_id end pd_uv_dev,
CASE WHEN t3.event_type='cart_success_uv'  THEN t3.device_id end cart_success_uv_dev				 
from  dim.dim_vova_merchant t1
left join (select * from dim.dim_vova_goods where is_on_sale=1) t2 on t1.mct_id=t2.mct_id
left join 
(select element_id virtual_goods_id,device_id,'pdbuy_uv' event_type from dwd.dwd_vova_log_common_click where pt='${cur_date}' and element_name ='pdAddToCartClick' and element_id !='' and element_id is not null
union 
select cast(virtual_goods_id as string) virtual_goods_id,device_id,'pd_uv' event_type  from dwd.dwd_vova_log_screen_view where pt='${cur_date}' and virtual_goods_id is not null  and  page_code='product_detail'  and view_type='show'
union
select element_id virtual_goods_id,device_id,'cart_success_uv' event_type from dwd.dwd_vova_log_common_click where pt='${cur_date}' and page_code='product_detail' and element_name='pdAddToCartSuccess'  and element_id is not null and element_id !=''
)t3
on t2.virtual_goods_id=t3.virtual_goods_id
)tmp3 group by mct_id
)tmp4
on tmp1.mct_id=tmp4.mct_id 

left join
(
select t3.mct_id,cast(t3.on_sale_dt as string) on_sale_dt from
(
select t2.mct_id,t2.on_sale_dt,row_number() over(partition by t2.mct_id order by t2.on_sale_dt desc) rn from
(
select g.mct_id,t1.on_sale_dt from dim.dim_vova_goods g left join
(select goods_id,date(create_time) on_sale_dt from ods_vova_vts.ods_vova_goods_on_sale_record where goods_id in
 (select goods_id from ods_vova_vts.ods_vova_goods_on_sale_record group by goods_id having count(*)=1)
)t1
on g.goods_id=t1.goods_id
)t2
)t3
where rn=1
)tmp5
on tmp1.mct_id=tmp5.mct_id  order by gmv desc limit 1000
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=dwb_main_report" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
--conf "spark.sql.autoBroadcastJoinThreshold=52428800" \
-e "$sql"


if [ $? -ne 0 ]; then
  echo "商铺主流程统计${cur_date}错误"
  exit 1
fi

