#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
insert overwrite table dws.dws_vova_buyer_goods_behave partition(pt ='${pre_date}')
select
/*+ REPARTITION(30) */
/*+ BROADCAST(prt) */
buyer_id,
gs_id,
max(cat_id) as cat_id,
max(first_cat_id) as first_cat_id,
max(second_cat_id) as second_cat_id,
max(brand_id) as brand_id,
max(price_range) as price_range,
max(expre_cnt) as expre_cnt,
max(clk_cnt) as clk_cnt,
max(clk_valid_cnt) clk_valid_cnt,
max(collect_cnt) as collect_cnt,
max(add_cat_cnt) as add_cat_cnt,
max(ord_cnt) as ord_cnt,
max(sales_vol) as sales_vol,
max(gmv) as gmv
from
(select
tmp_all.buyer_id,
tmp_all.gs_id,
dg.cat_id,
dg.first_cat_id,
dg.second_cat_id,
dg.brand_id,
prt.id as price_range,
tmp_all.expre_cnt,
tmp_all.clk_cnt,
tmp_all.clk_valid_cnt,
tmp_all.collect_cnt,
tmp_all.add_cat_cnt,
tmp_all.ord_cnt,
tmp_all.sales_vol,
tmp_all.gmv
from
  (select
  nvl(tmp_sp.buyer_id,tmp_ord.buyer_id) as buyer_id,
  nvl(tmp_sp.gs_id,tmp_ord.gs_id) as gs_id,
  nvl(tmp_sp.expre_cnt,0) as expre_cnt ,
  nvl(tmp_sp.clk_cnt,0) as clk_cnt,
  nvl(tmp_sp.clk_valid_cnt,0) as clk_valid_cnt,
  nvl(tmp_sp.collect_cnt,0) as collect_cnt,
  nvl(tmp_sp.add_cat_cnt,0) as add_cat_cnt,
  nvl(tmp_ord.ord_cnt,0) as ord_cnt,
  nvl(tmp_ord.sales_vol,0) as sales_vol,
  nvl(tmp_ord.gmv,0) as gmv
  from
      (select
      tmp_expre.buyer_id,
      dg.goods_id as gs_id,
      tmp_expre.expre_cnt,
      tmp_clk.clk_cnt,
      tmp_clk.clk_valid_cnt,
      tmp_add_cat.collect_cnt,
      tmp_add_cat.add_cat_cnt
      from
      (
      select
      gi.buyer_id,
      gi.virtual_goods_id as vir_gs_id,
      count(*) as expre_cnt
      from
      dwd.dwd_vova_log_goods_impression gi
      where gi.pt = '${pre_date}' and platform ='mob'
      group by gi.buyer_id,gi.virtual_goods_id) tmp_expre
      left join
      -- 点击数数据
      (select
      gc.buyer_id,
      gc.virtual_goods_id as vir_gs_id,
      count(*) as clk_cnt,
      count(*) as clk_valid_cnt
      from dwd.dwd_vova_log_goods_click gc
      where gc.pt = '${pre_date}'
      group by gc.buyer_id,gc.virtual_goods_id)tmp_clk
      on tmp_expre.buyer_id = tmp_clk.buyer_id and tmp_expre.vir_gs_id = tmp_clk.vir_gs_id
      left join

      -- 加车收藏数据
      (select
      cc.buyer_id,
      cast(cc.element_id as bigint) as vir_gs_id,
      count(*) as clk_cnt,
      count(*) as clk_valid_cnt,
      sum(if(cc.element_name ='pdAddToWishlistClick',1,0)) as collect_cnt,
      sum(if(cc.element_name ='pdAddToCartSuccess',1,0)) as add_cat_cnt
      from
      dwd.dwd_vova_log_common_click cc
      where cc.pt = '${pre_date}' and platform ='mob'
      group by cc.buyer_id,cc.element_id)tmp_add_cat
      on tmp_expre.buyer_id = tmp_add_cat.buyer_id and tmp_expre.vir_gs_id = tmp_add_cat.vir_gs_id
      inner join dim.dim_vova_goods dg
      on tmp_expre.vir_gs_id = dg.virtual_goods_id
  )tmp_sp
  full join
-- 购买数据
  (select
  buyer_id,
  goods_id gs_id,
  count(*) as ord_cnt,
  sum(goods_number) as sales_vol,
  sum(shop_price*goods_number+shipping_fee) as gmv
  from dwd.dwd_vova_fact_pay fp
  where to_date(order_time) = '${pre_date}' and fp.platform in ('ios','android')
  group by buyer_id,goods_id
  )tmp_ord
  on tmp_sp.buyer_id = tmp_ord.buyer_id and tmp_sp.gs_id = tmp_ord.gs_id
)tmp_all
inner join dim.dim_vova_goods dg
on tmp_all.gs_id = dg.goods_id
left join tmp.tmp_vova_dictionary_price_range_type prt
on (dg.shop_price+dg.shipping_fee) >=prt.min_val and (dg.shop_price+dg.shipping_fee) <prt.max_val
where  not exists (select distinct goods_id from ads.ads_vova_goods_black_list t1 where tmp_all.gs_id = t1.goods_id))
group by buyer_id,gs_id
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--driver-memory 10G \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
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
