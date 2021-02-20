#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
set hive.exec.dynamic.partition.mode=nonstrict;
with tmp_imp(
select
goods_id,
page_code,
list_type,
rp_id,
count(*) as expre_cnt
from
(select
gi.pt,
dg.goods_id,
gi.page_code,
gi.list_type,
explode(split(get_rp_name(gi.recall_pool),',')) rp_id
from
dwd.dwd_vova_log_goods_impression gi
inner join dim.dim_vova_goods dg on gi.virtual_goods_id = dg.virtual_goods_id
where gi.pt='${pre_date}'
and gi.datasource='vova')
group by goods_id,page_code,list_type,rp_id),

tmp_clk(
select
goods_id,
page_code,
list_type,
rp_id,
count(*) as clk_cnt
from
(select
gi.pt,
dg.goods_id,
gi.page_code,
gi.list_type,
explode(split(get_rp_name(gi.recall_pool),',')) rp_id
from
dwd.dwd_vova_log_goods_click gi
inner join dim.dim_vova_goods dg on gi.virtual_goods_id = dg.virtual_goods_id
where gi.pt='${pre_date}'
and gi.datasource='vova')
group by goods_id,page_code,list_type,rp_id),

tmp_add_cart(
select
goods_id,
page_code,
list_type,
rp_id,
count(*) as add_cart_cnt
from
(select
ccv.pt,
dg.goods_id,
ccv.pre_page_code as page_code,
ccv.pre_list_type as list_type,
explode(split(get_rp_name(ccv.pre_recall_pool),',')) rp_id
from
dwd.dwd_vova_fact_cart_cause_v2 ccv
inner join dim.dim_vova_goods dg on ccv.virtual_goods_id = dg.virtual_goods_id
where ccv.pt='${pre_date}'
and ccv.datasource='vova')
group by goods_id,page_code,list_type,rp_id),

tmp_pay(
select
goods_id,
page_code,
list_type,
rp_id,
count(*) as order_cnt,
sum(total_amount) as gmv
from
(select
ocv.pt,
fp.goods_id,
ocv.pre_page_code as page_code,
ocv.pre_list_type as list_type,
(fp.shop_price*fp.goods_number+fp.shipping_fee) as total_amount,
explode(split(get_rp_name(ocv.pre_recall_pool),',')) rp_id
from
dwd.dwd_vova_fact_order_cause_v2 ocv
inner join dwd.dwd_vova_fact_pay fp on ocv.order_goods_id = fp.order_goods_id
where ocv.pt='${pre_date}'
and ocv.datasource='vova')
group by goods_id,page_code,list_type,rp_id)

insert overwrite table ads.ads_vova_goods_imp_detail partition(pt='${pre_date}',gpt)
select
/*+ COALESCE(1) */
tmp_imp.goods_id,
tmp_imp.page_code,
tmp_imp.list_type,
tmp_imp.rp_id,
tmp_imp.expre_cnt,
nvl(tmp_clk.clk_cnt,0) as clk_cnt,
nvl(tmp_add_cart.add_cart_cnt,0) as add_cart_cnt,
nvl(tmp_pay.order_cnt,0) as order_cnt,
nvl(tmp_pay.gmv,0) as gmv,
cast(substr(tmp_imp.goods_id,4) as int)%200 as gpt
from
tmp_imp
left join tmp_clk on tmp_imp.goods_id = tmp_clk.goods_id and tmp_imp.page_code = tmp_clk.page_code and tmp_imp.list_type = tmp_clk.list_type and tmp_imp.rp_id = tmp_clk.rp_id
left join tmp_add_cart on tmp_imp.goods_id = tmp_add_cart.goods_id and tmp_imp.page_code = tmp_add_cart.page_code and tmp_imp.list_type = tmp_add_cart.list_type and tmp_imp.rp_id = tmp_add_cart.rp_id
left join tmp_pay on tmp_imp.goods_id = tmp_pay.goods_id and tmp_imp.page_code = tmp_pay.page_code and tmp_imp.list_type = tmp_pay.list_type and tmp_imp.rp_id = tmp_pay.rp_id
-- left join ods.recall_pool_code_name t1 on t1.id = tmp_imp.rp_id
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_goods_imp_detail" \
--conf "spark.default.parallelism=380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=200000" \
--conf "spark.network.timeout=300" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
