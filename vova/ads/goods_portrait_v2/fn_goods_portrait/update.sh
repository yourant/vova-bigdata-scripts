#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

sql="
insert overwrite table ads.ads_fn_goods_portrait partition(pt='${cur_date}')
select
tmp_all.gs_id,
'florynight' as datasource,
dg.cat_id,
dg.first_cat_id,
dg.shop_price,
tmp_all.expre_cnt as expre_cnt_1w,
tmp_all.clk_cnt as clk_cnt_1w,
tmp_all.add_cat_cnt as add_cat_cnt_1w,
tmp_all.collect_cnt as collect_cnt_1w,
tmp_all.sales_vol as sales_vol_1w,
tmp_all.ord_cnt,
tmp_all.gmv as gmv_1w
from
  (select
  nvl(tmp_sp.gs_id,tmp_ord.gs_id) as gs_id,
  nvl(tmp_sp.expre_cnt,0) as expre_cnt ,
  nvl(tmp_sp.clk_cnt,0) as clk_cnt,
  nvl(tmp_sp.collect_cnt,0) as collect_cnt,
  nvl(tmp_sp.add_cat_cnt,0) as add_cat_cnt,
  nvl(tmp_ord.ord_cnt,0) as ord_cnt,
  nvl(tmp_ord.sales_vol,0) as sales_vol,
  nvl(tmp_ord.gmv,0) as gmv
  from
      (select
      dg.goods_id as gs_id,
      tmp_expre.expre_cnt,
      tmp_clk.clk_cnt,
      tmp_clk.clk_valid_cnt,
      tmp_add_cat.collect_cnt,
      tmp_add_cat.add_cat_cnt
      from
      (
      select
      gi.virtual_goods_id as vir_gs_id,
      count(*) as expre_cnt
      from
      dwd.dwd_vova_log_goods_impression gi
      where gi.pt <= '${cur_date}' and gi.pt> date_sub('${cur_date}',7) and gi.datasource='florynight'
      group by gi.virtual_goods_id) tmp_expre
      left join
      -- 点击数数据
      (select
      gc.virtual_goods_id as vir_gs_id,
      count(*) as clk_cnt,
      count(*) as clk_valid_cnt
      from dwd.dwd_vova_log_goods_click gc
      where gc.pt <= '${cur_date}' and gc.pt> date_sub('${cur_date}',7) and gc.datasource='florynight'
      group by gc.virtual_goods_id)tmp_clk
      on tmp_expre.vir_gs_id = tmp_clk.vir_gs_id
      left join

      -- 加车收藏数据
      (select
      cast(cc.element_id as bigint) as vir_gs_id,
      sum(if(cc.element_name ='AddToWishlistSuccess',1,0)) as collect_cnt,
      sum(if(cc.element_name ='AddToCartSuccess',1,0)) as add_cat_cnt
      from
      dwd.dwd_vova_log_data cc
      where cc.pt <= '${cur_date}' and cc.pt> date_sub('${cur_date}',7) and cc.datasource='florynight'
      group by cc.element_id)tmp_add_cat
      on tmp_expre.vir_gs_id = tmp_add_cat.vir_gs_id
      inner join dim.dim_zq_goods dg
      on tmp_expre.vir_gs_id = dg.virtual_goods_id
  )tmp_sp
  full join
-- 购买数据
  (select
    goods_id gs_id,
    count(*) as ord_cnt,
    sum(goods_number) as sales_vol,
    sum(shop_price*goods_number) as gmv
    from ods_zq_zsp.ods_zq_order_goods og
    left join ods_zq_zsp.ods_zq_order_info oi
    on og.order_id = oi.order_id
    where oi.pay_status >= 1
    AND to_date(order_time) <= '${cur_date}'
    and to_date(order_time)> date_sub('${cur_date}',7)
  group by goods_id
  )tmp_ord
  on tmp_sp.gs_id = tmp_ord.gs_id
)tmp_all
inner join dim.dim_zq_goods dg
on tmp_all.gs_id = dg.goods_id
"

spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_fn_goods_portrait" \
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
if [ $? -ne 0 ];then
  exit 1
fi


