#!/bin/bash
#指定日期和引擎
pre_date=$1
pre3_date=$2
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-3 day" +%Y-%m-%d)
fi
sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws.dws_vova_buyer_goods_behave partition(pt)
select
/*+ REPARTITION(30) */
/*+ BROADCAST(prt) */
/*+ BROADCAST(tbl) */
buyer_id,
gs_id,
dg.cat_id as cat_id,
dg.first_cat_id,
dg.second_cat_id,
dg.brand_id,
prt.id as price_range,
expre_cnt,
clk_cnt,
clk_valid_cnt,
collect_cnt,
add_cat_cnt,
ord_cnt,
sales_vol,
gmv,
pt
from
(select
/*+ REPARTITION(500) */
pt,
buyer_id,
gs_id,
sum(expre_cnt) as expre_cnt,
sum(clk_cnt) as clk_cnt,
sum(clk_valid_cnt) clk_valid_cnt,
sum(collect_cnt) as collect_cnt,
sum(add_cat_cnt) as add_cat_cnt,
sum(ord_cnt) as ord_cnt,
sum(sales_vol) as sales_vol,
sum(gmv) as gmv
from
    (-- 曝光
     select
          gi.pt,
          gi.buyer_id,
          dg.goods_id as gs_id,
          count(*) as expre_cnt,
          0 as clk_cnt,
          0 as clk_valid_cnt,
          0 as collect_cnt,
          0 as add_cat_cnt,
          0 as ord_cnt,
          0 as sales_vol,
          0 as gmv
          from
          dwd.dwd_vova_log_goods_impression gi
          inner join dim.dim_vova_goods dg on gi.virtual_goods_id = dg.virtual_goods_id
          where gi.pt <= '${pre_date}' and gi.pt >= '${pre3_date}'  and platform ='mob'
          group by gi.buyer_id,dg.goods_id,gi.pt
    union all
    -- 点击
    select
          gc.pt,
          gc.buyer_id,
          dg.goods_id as gs_id,
          0 as expre_cnt,
          count(*) as clk_cnt,
          count(*) as clk_valid_cnt,
          0 as collect_cnt,
          0 as add_cat_cnt,
          0 as ord_cnt,
          0 as sales_vol,
          0 as gmv
          from dwd.dwd_vova_log_goods_click gc
          inner join dim.dim_vova_goods dg on gc.virtual_goods_id = dg.virtual_goods_id
          where gc.pt <= '${pre_date}' and gc.pt >= '${pre3_date}'
          group by gc.buyer_id,dg.goods_id,gc.pt
    union all
    --收藏加购
    select
          cc.pt,
          cc.buyer_id,
          dg.goods_id as gs_id,
          0 as expre_cnt,
          0 as clk_cnt,
          0 as clk_valid_cnt,
          sum(if(cc.element_name ='pdAddToWishlistClick',1,0)) as collect_cnt,
          sum(if(cc.element_name ='pdAddToCartSuccess',1,0)) as add_cat_cnt,
          0 as ord_cnt,
          0 as sales_vol,
          0 as gmv
          from
          dwd.dwd_vova_log_common_click cc
          inner join dim.dim_vova_goods dg on cc.element_id = dg.virtual_goods_id
          where cc.pt <= '${pre_date}' and cc.pt >= '${pre3_date}' and platform ='mob'
          group by cc.buyer_id,dg.goods_id,cc.pt
    union all
    -- 购买信息
    select
          to_date(order_time) as pt,
          buyer_id,
          goods_id gs_id,
          0 as expre_cnt,
          0 as clk_cnt,
          0 as clk_valid_cnt,
          0 as collect_cnt,
          0 as add_cat_cnt,
          count(*) as ord_cnt,
          sum(goods_number) as sales_vol,
          sum(shop_price*goods_number+shipping_fee) as gmv
      from dwd.dwd_vova_fact_pay fp
      where to_date(order_time) <= '${pre_date}' and to_date(order_time) >= '${pre3_date}' and fp.platform in ('ios','android')
      group by buyer_id,goods_id,to_date(order_time))
group by buyer_id,gs_id,pt)tmp_all
    inner join dim.dim_vova_goods dg on tmp_all.gs_id = dg.goods_id
    left join tmp.tmp_vova_dictionary_price_range_type prt
     on (dg.shop_price+dg.shipping_fee) >=prt.min_val and (dg.shop_price+dg.shipping_fee) <prt.max_val
where  not exists (select 1 from ads.ads_vova_goods_black_list tbl where tmp_all.gs_id = tbl.goods_id)
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--driver-memory 10G \
--executor-memory 12G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=dws_vova_buyer_goods_behave" \
--conf "spark.default.parallelism = 280" \
--conf "spark.sql.shuffle.partitions=280" \
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