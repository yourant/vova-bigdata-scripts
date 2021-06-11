#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
insert overwrite table ads.ads_vova_recall_page_analysis partition(pt='${pre_date}')
select
p_type,
if(t2.comment is not null and t2.comment != '',t2.comment,t1.version) as version,
sum(expre_cnt) as expre_cnt,
if(sum(pay_uv)/sum(exp_uv)<1,sum(pay_uv)/sum(exp_uv) ,1 ) as rate
from
(select
p_type,
version,
count(1)  as expre_cnt,
count(distinct device_id) as exp_uv,
0 as pay_uv
from
(
select
if(concat(page_code,list_type) in ('homepage/popular','homepage/product_list_popular','product_list/product_list_popular','product_list/product_list_price_asc','product_list/product_list_sold','search_result/search_result_recommend'
,'search_result/search_result_also_like','search_result/search_result_sold','search_result/search_result_price_asc','cart/cart_also_like','me/me_also_like','product_detail/detail_also_like','image_search_result/image_search_recommend'
,'coins_rewards/coins_rewards','my_favorites/favorites','my_order/my_order_also_like','my_orders/my_orders_also_like','order_tracking/order_tracking_also_like','payment_success/pay_success'),concat(page_code,list_type),'others') as p_type,
device_id,
explode(split(get_rp_name(recall_pool),',')) as version
from
dwd.dwd_vova_log_goods_impression where pt='${pre_date}'
)
group by
p_type,
version

union all

select
p_type,
version,
0  as expre_cnt,
0 as exp_uv,
count(distinct device_id) as pay_uv
from
(select
if(concat(pre_page_code,pre_list_type) in ('homepage/popular','homepage/product_list_popular','product_list/product_list_popular','product_list/product_list_price_asc','product_list/product_list_sold','search_result/search_result_recommend'
,'search_result/search_result_also_like','search_result/search_result_sold','search_result/search_result_price_asc','cart/cart_also_like','me/me_also_like','product_detail/detail_also_like','image_search_result/image_search_recommend'
,'coins_rewards/coins_rewards','my_favorites/favorites','my_order/my_order_also_like','my_orders/my_orders_also_like','order_tracking/order_tracking_also_like','payment_success/pay_success'),concat(pre_page_code,pre_list_type),'others') as p_type,
oc.device_id,
explode(split(get_rp_name(oc.pre_recall_pool),',')) as version
from
dwd.dwd_vova_fact_order_cause_v2 oc
inner join dwd.dwd_vova_fact_pay fp on oc.order_goods_id = fp.order_goods_id
where pt='${pre_date}'
)
group by
p_type,
version)t1
left join ods_vova_ext.recall_pool_code_name t2 on t1.version = t2.id
group by
p_type,
if(t2.comment is not null and t2.comment != '',t2.comment,t1.version)
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_recall_page_analysis" \
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

if [ $? -ne 0 ];then
  exit 1
fi
